/*-------------------------------------------------------------------------
 *
 * undorecordset.c
 *	  management of sets of records in undo logs
 *
 * An UndoRecordSet object is used to manage the creation of a set of
 * related undo records on disk. Typically, this corresponds to all the
 * records written by a single transaction for a single persistence
 * level (permanent, temporary, unlogged) but we don't assume that here,
 * since other uses of the undo storage mechanism are possible.
 *
 * Multiple undo record sets may be written within a single undo log,
 * and a single undo record set may span multiple undo logs. The latter
 * is fairly uncommon, because undo logs are big (1TB) and most
 * transactions will write far less than that amount of undo. A
 * single undo record, however, cannot span multiple undo logs. An
 * undo record set on disk therefore consists of a series of one or more
 * chunks, each of which consists of a chunk header and one or more of
 * records, and the first of which also has a type-specific header
 * containing whatever data is needed for the particular type of record
 * set that it is. For example, if it belongs to a transaction, the
 * type-specific header will contain the transaction ID. The
 * type-specific header and chunk header are written at the same time
 * as the first record in the chunk so as to minimize WAL volume.
 *
 * Every undo record set that is created must be properly closed,
 * for two principal reasons.  First, if any records have been written
 * to disk, the final size of the last chunk must be set on disk; by
 * convention, the last undo record set within an undo log may have
 * a size of 0, indicating that data is still being written, but all
 * previous ones must have a correct size. Second, while one backend
 * is writing to an undo record set, no other backend can write to
 * the same undo log, since record sets are not interleaved; closing
 * the undo record set makes that undo log available for reuse.
 * In the event of a crash, undolog.c will put all undo logs back on
 * the free list. Also, the last chunk in each undo log will be inspected
 * to see whether the size is 0; if so, the size will be set based on the
 * insert pointer for that undo log.
 *
 * Clients of this module are responsible for ensuring that undo record
 * sets are closed in all cases that do not involve a system crash.
 * If they fail to do so, this module will trigger a PANIC at backend
 * exit; the crash recovery algorithm described above should get
 * things back to a sane state.
 *
 * Code that wants to write transactional undo should interface with
 * xactundo.c, q.v., rather than calling these interfaces directly.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/undo/undorecordset.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/undo.h"
#include "access/undolog.h"
#include "access/undolog_xlog.h"
#include "access/undopage.h"
#include "access/undorecordset.h"
#include "access/undorecordset_xlog.h"
#include "access/xactundo.h"	/* XXX should we avoid this? */
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "catalog/pg_class.h"
#include "catalog/pg_control.h"
#include "common/undo_parser.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/buf.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"

/*
 * Per-chunk bookkeeping.
 */
typedef struct UndoRecordSetChunk
{
	UndoLogSlot *slot;
	bool		chunk_header_written;
	/* The offset of the chunk header. */
	UndoLogOffset chunk_header_offset;
	/* The index of the one or two buffers that hold the size. */
	int			chunk_header_buffer_index[2];
} UndoRecordSetChunk;

typedef enum UndoRecordSetState
{
	URS_STATE_CLEAN,			/* has written no data */
	URS_STATE_DIRTY,			/* has written some data */
	URS_STATE_CLOSED			/* wrote data and was then closed */
} UndoRecordSetState;

typedef struct UndoBuffer
{
	Buffer		buffer;
	bool		is_new;
	bool		needs_init;
	uint16		init_page_offset;
	UndoRecordSetXLogBufData bufdata;
} UndoBuffer;

struct UndoRecordSet
{
	UndoRecordSetType type;
	char		persistence;

	/*
	 * Management of chunks used when inserting.  Typically there will only be
	 * one, but when the end of the address space in an undo log is reached,
	 * we may need to wrap into another.
	 */
	UndoRecordSetChunk *chunks;
	int			nchunks;
	int			max_chunks;

	/* Management of currently pinned and locked buffers. */
	UndoBuffer *buffers;
	int			nbuffers;
	int			max_buffers;

	/*
	 * UndoPrepareToInsert's decision on headers for the in-progress
	 * insertion.
	 */
	UndoRecPtr	previous_chunk;
	bool		need_chunk_header;
	char	   *type_header;
	uint8		type_header_size;
	bool		need_type_header;
	UndoRecPtr	begin;

	/* Currently active slot for insertion. */
	UndoLogSlot *slot;
	UndoRecPtr	chunk_start;	/* where the chunk started */

	/* Resource management. */
	UndoRecordSetState state;
	slist_node	link;
	int			nestingLevel;
};

#define URSNeedsWAL(urs) ((urs)->persistence == RELPERSISTENCE_PERMANENT)

static inline void reserve_buffer_array(UndoRecordSet *urs, size_t capacity);

/* Every UndoRecordSet created and not yet destroyed in this backend. */
static slist_head UndoRecordSetList = SLIST_STATIC_INIT(UndoRecordSetList);

/*
 * Create a new UndoRecordSet with the indicated type and persistence level.
 *
 * The persistence level may be RELPERSISTENCE_TEMP, RELPERSISTENCE_UNLOGGED,
 * or RELPERSISTENCE_PERMANENT.
 *
 * An UndoRecordSet is created using this function must be properly closed;
 * see UndoPrepareToMarkClosed and UndoMarkClosed. If nestingLevel > 0, the
 * UndoRecordSet will automatically be closed when the transaction nesting
 * depth drops below this value, unless it has been previously closed
 * explicitly. Even if you plan to close the UndoRecordSet explicitly in
 * normal cases, the use of this facility is advisable to make sure that
 * the UndoRecordSet is closed even in case of ERROR or FATAL.
 */
UndoRecordSet *
UndoCreate(UndoRecordSetType type, char persistence, int nestingLevel,
		   Size type_header_size, char *type_header)
{
	UndoRecordSet *urs;
	MemoryContext oldcontext;

	if (IsInParallelMode())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("cannot write undo during a parallel operation")));

	Assert(UndoContext != NULL);

	oldcontext = MemoryContextSwitchTo(UndoContext);
	urs = palloc0(sizeof(UndoRecordSet));
	urs->type = type;
	urs->persistence = persistence;
	urs->chunks = palloc(sizeof(urs->chunks[0]));
	urs->max_chunks = 1;
	urs->buffers = palloc(sizeof(urs->buffers[0]));
	urs->max_buffers = 1;
	urs->type_header_size = type_header_size;
	urs->need_type_header = true;

	urs->type_header = palloc(type_header_size);
	memcpy(urs->type_header, type_header, type_header_size);
	urs->type_header_size = type_header_size;

	slist_push_head(&UndoRecordSetList, &urs->link);
	urs->nestingLevel = nestingLevel;
	MemoryContextSwitchTo(oldcontext);

	return urs;
}

/*
 * Return the index in urs->buffers of the requested buffer, or create a new
 * one.
 */
static int
find_or_read_buffer(UndoRecordSet *urs, UndoLogNumber logno, BlockNumber block)
{
	RelFileNode rnode;

	/* Do we have it pinned and locked already? */
	for (int i = 0; i < urs->nbuffers; ++i)
	{
		ForkNumber	tag_fork;
		BlockNumber tag_block;
		SmgrId		smgrid;

		BufferGetTag(urs->buffers[i].buffer, &smgrid, &rnode, &tag_fork,
					 &tag_block);
		if (smgrid == SMGR_UNDO && rnode.relNode == logno &&
			tag_block == block)
			return i;
	}

	/* Pin it and lock it. */
	reserve_buffer_array(urs, urs->nbuffers + 1);
	memset(&urs->buffers[urs->nbuffers], 0, sizeof(urs->buffers[0]));
	UndoRecPtrAssignRelFileNode(rnode, MakeUndoRecPtr(logno, 0));
	urs->buffers[urs->nbuffers].buffer =
		ReadBufferWithoutRelcache(SMGR_UNDO,
								  rnode,
								  UndoLogForkNum,
								  block,
								  RBM_NORMAL,
								  NULL,
								  urs->persistence);
	LockBuffer(urs->buffers[urs->nbuffers].buffer, BUFFER_LOCK_EXCLUSIVE);

	return urs->nbuffers++;
}

static void
UndoPrepareToMarkChunkClosed(UndoRecordSet *urs, UndoRecordSetChunk *chunk)
{
	UndoLogOffset header;
	BlockNumber header_block;
	int			header_offset;

	/* Find the header of this chunk. */
	header = chunk->chunk_header_offset;
	header_block = header / BLCKSZ;
	header_offset = header % BLCKSZ;

	/*
	 * We might need one or two buffers, depending on the position of the
	 * header.  We might need to read a new buffer, but to support inserting
	 * and closing as part of the same WAL record, we also have to check if we
	 * already have the buffer pinned.
	 */
	chunk->chunk_header_buffer_index[0] =
		find_or_read_buffer(urs, chunk->slot->logno, header_block);
	if (header_offset <= BLCKSZ - sizeof(UndoLogOffset))
		chunk->chunk_header_buffer_index[1] = -1;
	else
		chunk->chunk_header_buffer_index[1] =
			find_or_read_buffer(urs, chunk->slot->logno, header_block + 1);
}

/*
 * Pin and lock the buffers that hold the active chunk's header, in
 * preparation for marking it closed.
 *
 * Returns 'true' if work needs to be done and 'false' if not. If the return
 * value is 'false', it is acceptable to call UndoDestroy without doing
 * anything further.
 */
bool
UndoPrepareToMarkClosed(UndoRecordSet *urs)
{
	if (urs->nchunks == 0)
		return false;

	UndoPrepareToMarkChunkClosed(urs, &urs->chunks[urs->nchunks - 1]);

	return true;
}

/*
 * Do the per-page work associated with marking an UndoRecordSet closed.
 */
static int
UndoMarkPageClosed(UndoRecordSet *urs, UndoRecordSetChunk *chunk, int chbidx,
				   int page_offset, int data_offset, UndoLogOffset size)
{
	int			index = chunk->chunk_header_buffer_index[chbidx];
	Buffer		buffer = urs->buffers[index].buffer;
	int			bytes_on_this_page;

	/* Update the page. */
	bytes_on_this_page =
		UndoPageOverwrite(BufferGetPage(buffer),
						  page_offset,
						  data_offset,
						  sizeof(size),
						  (char *) &size);
	MarkBufferDirty(buffer);

	return bytes_on_this_page;
}

static void
UndoMarkChunkClosed(UndoRecordSet *urs, UndoRecordSetChunk *chunk,
					bool close_urs)
{
	UndoLogOffset header;
	UndoLogOffset insert;
	UndoLogOffset size;
	int			page_offset;
	int			data_offset;
	int			chbidx;

	/* Must be in a critical section. */
	Assert(CritSectionCount > 0);

	/* Must have prepared the buffers for this. */
	Assert(chunk->chunk_header_buffer_index[0] != -1);

	header = chunk->chunk_header_offset;
	insert = chunk->slot->meta.insert;
	size = insert - header;
	page_offset = header % BLCKSZ;
	data_offset = 0;
	chbidx = 0;

	/* Record the close as bufdata on the first affected page. */
	if (URSNeedsWAL(urs))
	{
		UndoBuffer *ubuf;

		ubuf = &urs->buffers[chunk->chunk_header_buffer_index[0]];
		ubuf->bufdata.flags |= URS_XLOG_CLOSE_CHUNK;
		ubuf->bufdata.chunk_size_page_offset = page_offset;
		ubuf->bufdata.chunk_size = size;

		/*
		 * If we're closing the final chunk and thus the whole URS, we need to
		 * log some extra details.
		 */
		if (close_urs)
		{
			ubuf->bufdata.flags |= URS_XLOG_CLOSE;
			ubuf->bufdata.urs_type = urs->type;
			ubuf->bufdata.type_header = urs->type_header;
			ubuf->bufdata.type_header_size = urs->type_header_size;
		}
	}

	while (data_offset < sizeof(size))
	{
		data_offset += UndoMarkPageClosed(urs, chunk, chbidx++,
										  page_offset, data_offset, size);
		page_offset = SizeOfUndoPageHeaderData;
	}
}

/*
 * Mark an undo record set closed.
 *
 * This should be called from the critical section, after having first called
 * UndoPrepareToMarkClosed before establishing the critical section.
 */
void
UndoMarkClosed(UndoRecordSet *urs)
{
	UndoRecordSetChunk *chunk;

	/* Shouldn't already be closed, and should have chunks if it's dirty. */
	Assert(urs->state != URS_STATE_CLOSED);
	Assert(urs->state == URS_STATE_CLEAN || urs->nchunks != 0);

	if (urs->state == URS_STATE_DIRTY)
	{
		/* Locate the active chunk. */
		chunk = &urs->chunks[urs->nchunks - 1];
		UndoMarkChunkClosed(urs, chunk, true);

		urs->state = URS_STATE_CLOSED;
	}
}

/*
 * Prepare to set a boolean flag of the chunk (type) header.
 *
 * *buf receives the buffer containing the flag.
 */
void
UndoPrepareToSetChunkHeaderFlag(UndoRecPtr chunk_hdr, uint16 off,
								char persistence, Buffer *buf)
{
	RelFileNode rnode;
	BlockNumber block;
	UndoLogNumber	logno = UndoRecPtrGetLogNo(chunk_hdr);
	UndoLogOffset chunk_hdr_off = UndoRecPtrGetOffset(chunk_hdr);
	UndoRecPtr	flag_ptr;
	UndoLogOffset	flag_off;


	/* Find out where in the log is the flag stored. */
	flag_off = UndoLogOffsetPlusUsableBytes(chunk_hdr_off, off);
	flag_ptr = MakeUndoRecPtr(logno, flag_off);

	UndoRecPtrAssignRelFileNode(rnode, flag_ptr);
	block = UndoRecPtrGetBlockNum(flag_ptr);

	*buf = ReadBufferWithoutRelcache(SMGR_UNDO,
									 rnode,
									 UndoLogForkNum,
									 block,
									 RBM_NORMAL,
									 NULL,
									 persistence);
	LockBuffer(*buf, BUFFER_LOCK_EXCLUSIVE);
}

/*
 * Make sure we have enough space to hold a buffer array of a given size.
 */
static inline void
reserve_buffer_array(UndoRecordSet *urs, size_t capacity)
{
	if (unlikely(urs->max_buffers < capacity))
	{
		urs->buffers = repalloc(urs->buffers,
								sizeof(urs->buffers[0]) * capacity);
		urs->max_buffers = capacity;
	}
}

/*
 * Attach to a new undo log so that we can begin a new chunk.
 */
static void
create_new_chunk(UndoRecordSet *urs, UndoLogNumber min_logno)
{
	/* Make sure there is book-keeping space for one more chunk. */
	if (urs->nchunks == urs->max_chunks)
	{
		urs->chunks = repalloc(urs->chunks,
							   sizeof(urs->chunks[0]) * urs->max_chunks * 2);
		urs->max_chunks *= 2;
	}

	/* Get our hands on a new undo log. */
	urs->need_chunk_header = true;
	urs->slot = UndoLogAcquire(urs->persistence, min_logno);
	urs->chunks[urs->nchunks].slot = urs->slot;
	urs->chunks[urs->nchunks].chunk_header_written = false;
	urs->chunks[urs->nchunks].chunk_header_offset = urs->slot->meta.insert;
	urs->chunks[urs->nchunks].chunk_header_buffer_index[0] = -1;
	urs->chunks[urs->nchunks].chunk_header_buffer_index[1] = -1;
	urs->chunk_start = MakeUndoRecPtr(urs->slot->logno, urs->slot->meta.insert);
	urs->nchunks++;
}

/*
 * Return a pointer to an undo log span that is guaranteed to be backed by
 * enough physical space for the given number of bytes.  Return
 * InvalidUndoRecPtr if there is not enough space remaining in the current
 * active undo log, indicating that the caller needs to create a new chunk.
 */
static UndoRecPtr
reserve_physical_undo(UndoRecordSet *urs, size_t total_size)
{
	UndoLogOffset new_insert;
	UndoLogOffset size,
				insert,
				end;

	Assert(urs->nchunks >= 1);
	Assert(urs->chunks);

	/*
	 * Although this is in shared memory, it can only be set by this project
	 * (for testing) if we are currently attached to it, so it's safe to read
	 * it without locking.
	 */
	if (unlikely(urs->slot->force_truncate))
	{
		/*
		 * Issue the XLOG record first. If we first modified the size
		 * information in the shared memory, it could trigger some other
		 * operation (e.g. discarding by a background worker) that writes its
		 * own XLOG, and we might fail to write the truncation XLOG record
		 * before the XLOG of the other operation.
		 */
		UndoLogTruncate(urs->slot, urs->slot->meta.insert);

		urs->slot->meta.size = urs->slot->meta.insert;
		urs->slot->force_truncate = false;
		urs->slot = NULL;
		return InvalidUndoRecPtr;
	}

	LWLockAcquire(&urs->slot->meta_lock, LW_SHARED);
	end = urs->slot->end;
	size = urs->slot->meta.size;
	insert = urs->slot->meta.insert;
	LWLockRelease(&urs->slot->meta_lock);

	/* The log is full, possibly due to recent truncation. */
	if (insert >= size)
	{
		Assert(insert == size);
		urs->slot = NULL;
		return InvalidUndoRecPtr;
	}

	new_insert = UndoLogOffsetPlusUsableBytes(insert, total_size);
	if (new_insert <= end)
		return MakeUndoRecPtr(urs->slot->logno, urs->slot->meta.insert);

	/*
	 * Can we extend this undo log to make space?  Again, it's possible for
	 * end to advance concurrently, but UndoLogAdjustPhysicalRange() can deal
	 * with that.
	 */
	if (new_insert <= size)
	{
		UndoLogAdjustPhysicalRange(urs->slot->logno, 0, new_insert);
		return MakeUndoRecPtr(urs->slot->logno, urs->slot->meta.insert);
	}

	/*
	 * There is not enough space left for this record.  Truncate any remaining
	 * space, so that we stop trying to reuse this undo log, and a checkpoint
	 * will eventually give up its slot for reuse.
	 */
	UndoLogTruncate(urs->slot, insert);
	LWLockAcquire(&urs->slot->meta_lock, LW_SHARED);
	urs->slot->meta.size = insert;
	LWLockRelease(&urs->slot->meta_lock);
	urs->slot = NULL;
	return InvalidUndoRecPtr;
}

/*
 * Return a pointer to an undo log region backed by sufficient physical space
 * for a record of a given size to be inserted, and pin all buffers in the
 * region.
 *
 * This operation may also prepare to mark an existing URS chunk to be marked
 * closed due to lack of space, if a new one must be created.
 */
UndoRecPtr
UndoPrepareToInsert(UndoRecordSet *urs, size_t record_size)
{
	UndoRecPtr	begin;
	size_t		header_size;
	size_t		total_size;
	RelFileNode rnode;
	BlockNumber block;
	int			offset;
	int			chunk_number_to_close = -1;
	UndoLogNumber min_logno = InvalidUndoLogNumber;

	if (IsInParallelMode())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("cannot write undo during a parallel operation")));

	for (;;)
	{
		/* Figure out the total range we need to pin. */
		if (!urs->need_chunk_header)
			header_size = 0;
		else if (!urs->need_type_header)
			header_size = SizeOfUndoRecordSetChunkHeader;
		else
			header_size = SizeOfUndoRecordSetChunkHeader + urs->type_header_size;
		total_size = record_size + header_size;

		/* Try to use the active undo log, if there is one. */
		if (urs->slot)
		{
			/*
			 * If a new log will be needed, do not go backwards in the
			 * sequence of logs. Forward scan of the undo record set (e.g. for
			 * discarding or troubleshooting purposes) is a little bit easier
			 * if we find the chunks in defined order.
			 */
			min_logno = urs->slot->logno + 1;

			begin = reserve_physical_undo(urs, total_size);
			if (begin != InvalidUndoRecPtr)
				break;

			/*
			 * The active chunk is full.  We will prepare to mark it closed,
			 * if we had already written a chunk header.  It's possible that
			 * we haven't written anything in there at all, in which case we
			 * just mark the chunk as unused again (otherwise we'd later try
			 * and reference it on disk).
			 */
			if (urs->chunks[urs->nchunks - 1].chunk_header_written)
				chunk_number_to_close = urs->nchunks - 1;
			else
			{
				UndoLogRelease(urs->chunks[urs->nchunks - 1].slot);
				urs->nchunks--;
			}
		}

		/* We need to create a new chunk in a new undo log. */
		create_new_chunk(urs, min_logno);
	}

	/* Make sure our buffer array is large enough. */
	reserve_buffer_array(urs, total_size / BLCKSZ + 2);

	/* We'd better not have any pinned already. */
	Assert(urs->nbuffers == 0);

	/* Figure out which undo log we're in. */
	UndoRecPtrAssignRelFileNode(rnode, begin);
	block = UndoRecPtrGetBlockNum(begin);
	offset = UndoRecPtrGetPageOffset(begin);

	/* Loop, pinning buffers. */
	while (total_size > 0)
	{
		int			bytes_on_this_page;
		ReadBufferMode rbm;

		memset(&urs->buffers[urs->nbuffers], 0, sizeof(urs->buffers[0]));

		/*
		 * If we are writing the first data into this page, we don't need to
		 * read it from disk.  We can just get a zeroed buffer and initialize
		 * it.
		 */
		if (offset == SizeOfUndoPageHeaderData)
		{
			rbm = RBM_ZERO;
			urs->buffers[urs->nbuffers].is_new = true;
			urs->buffers[urs->nbuffers].needs_init = true;
			urs->buffers[urs->nbuffers].init_page_offset = 0;
		}
		else
			rbm = RBM_NORMAL;

		/*
		 * TODO: Andres doesn't like "without relcache" here.
		 *
		 * (Couldn't we just open the relation normally and use regular old
		 * ReadBuffer? In some earlier versions of the code, this was shared
		 * with the recovery path, but now UndoAllocateInRecovery is separate
		 * anyway.)
		 */
		/* Get a buffer. */
		urs->buffers[urs->nbuffers].buffer =
			ReadBufferWithoutRelcache(SMGR_UNDO,
									  rnode,
									  UndoLogForkNum,
									  block,
									  rbm,
									  NULL,
									  urs->persistence);

		/* How much to go? */
		bytes_on_this_page = Min(BLCKSZ - offset, total_size);
		total_size -= bytes_on_this_page;

		/* Advance to start of next page. */
		++block;
		offset = SizeOfUndoPageHeaderData;
		++urs->nbuffers;
	}

	/*
	 * Now loop to obtain the content locks.  This is done as a separate loop
	 * so that we don't hold a content lock while potentially evicting a page.
	 *
	 * TODO: This doesn't actually address Andres's complaint, which is that
	 * we will presumably still do the eviction above at a time when an AM
	 * like zheap already has content locks.
	 */
	for (int i = 0; i < urs->nbuffers; ++i)
		LockBuffer(urs->buffers[i].buffer, BUFFER_LOCK_EXCLUSIVE);

	/*
	 * Tell UndoInsert() where the first byte is (which may be pointing to a
	 * header).
	 */
	urs->begin = begin;

	/*
	 * If we determined that we had to close an existing chunk, do so now.  It
	 * was important to deal with the insertion first, because UndoReplay()
	 * assumes that the blocks used for inserting headers and record data are
	 * registered before blocks touched by incidental work like marking chunks
	 * closed.
	 */
	if (chunk_number_to_close >= 0)
		UndoPrepareToMarkChunkClosed(urs, &urs->chunks[chunk_number_to_close]);

	/*
	 * Tell the caller where the first byte it where it can write record data
	 * (ie after any headers that the caller doesn't know/care about).
	 */
	return UndoRecPtrPlusUsableBytes(begin, header_size);
}

static void
init_if_needed(UndoBuffer *ubuf)
{
	if (ubuf->needs_init)
	{
		UndoPageInit(BufferGetPage(ubuf->buffer));

		if (ubuf->init_page_offset)
		{
			UndoPageHeader uph = (UndoPageHeader) BufferGetPage(ubuf->buffer);

			uph->ud_insertion_point = ubuf->init_page_offset;
		}

		ubuf->needs_init = false;
	}
}

static void
register_insert_page_offset_if_needed(UndoBuffer *ubuf, uint16 insert_page_offset)
{
	/*
	 * For now, we record the insertion point for the first insertion by this
	 * WAL record into each buffer.  Later we could find ways to avoid having
	 * to do this, to cut down on registered buffer data in the WAL.
	 */
	if ((ubuf->bufdata.flags & URS_XLOG_INSERT) == 0)
	{
		ubuf->bufdata.insert_page_offset = insert_page_offset;
		ubuf->bufdata.flags |= URS_XLOG_INSERT;
	}
}

static void
register_new_page(UndoBuffer *ubuf,
				  UndoRecordSetType chunk_type,
				  UndoRecPtr chunk_header_location)
{
	ubuf->bufdata.flags |= URS_XLOG_ADD_PAGE;
	ubuf->bufdata.chunk_header_location = chunk_header_location;
	ubuf->bufdata.urs_type = chunk_type;
}

/*
 * Append data to an undo log.  The space must previously have been allocated
 * with UndoPrepareToInsert().
 */
void
UndoInsert(UndoRecordSet *urs,
		   void *record_data,
		   size_t record_size)
{
	int			bytes_written;
	int			input_offset;
	int			buffer_index;
	int			page_offset;
	int			type_header_size = urs->need_type_header ? urs->type_header_size : 0;
	int			chunk_header_size = urs->need_chunk_header ? SizeOfUndoRecordSetChunkHeader : 0;
	int			all_header_size = type_header_size + chunk_header_size;
	UndoRecordSetChunk *first_chunk;
	bool		first_insert;
	uint16		init_page_offset = 0;

	if (IsInParallelMode())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("cannot write undo during a parallel operation")));

	Assert(!InRecovery);
	Assert(CritSectionCount > 0);

	/* The caller must already have called UndoPrepareToInsert. */
	Assert(urs->slot);
	Assert(urs->nbuffers >= 1);

	/*
	 * We start of writing into the first buffer, at the offset that
	 * UndoPrepareToInsert provided.
	 */
	buffer_index = 0;
	page_offset = urs->begin % BLCKSZ;

	/* Can't be pointing into page header. */
	Assert(page_offset >= SizeOfUndoPageHeaderData);

	/* Is this the first insert in the current URS? */
	first_chunk = &urs->chunks[0];
	first_insert = urs->begin == MakeUndoRecPtr(first_chunk->slot->logno,
												first_chunk->chunk_header_offset);

	/*
	 * The first buffer of the temporary set needs to be initialized because
	 * the previous owner (backend) of the underlying log might have dropped
	 * the corresponding local buffer w/o writing it initialized to disk. The
	 * initialization should not make any harm to that previous owner because
	 * its transaction should either have been rolled back or committed. Even
	 * if it was committed, the undo log should not be needed for visibility
	 * purposes because other transactions do not see the temporary relations
	 * anyway.
	 */
	if (urs->persistence == RELPERSISTENCE_TEMP &&
		buffer_index == 0 && first_insert)
		init_page_offset = page_offset;

	/* Write out the header(s), if necessary. */
	if (urs->need_chunk_header)
	{
		UndoRecordSetChunkHeader chunk_header;

		/* Initialize the chunk header. */
		chunk_header.size = 0;
		chunk_header.previous_chunk = InvalidUndoRecPtr;
		chunk_header.discarded = false;
		chunk_header.type = urs->type;

		if (urs->nchunks > 1)
		{
			UndoRecordSetChunk *prev_chunk;

			prev_chunk = &urs->chunks[urs->nchunks - 2];
			chunk_header.previous_chunk =
				MakeUndoRecPtr(prev_chunk->slot->logno,
							   prev_chunk->chunk_header_offset);
		}

		input_offset = 0;
		for (;;)
		{
			UndoBuffer *ubuf = &urs->buffers[buffer_index];

			if (buffer_index >= urs->nbuffers)
				elog(ERROR, "ran out of buffers while inserting undo record headers");
			if (init_page_offset > 0)
			{
				ubuf->needs_init = true;
				ubuf->init_page_offset = init_page_offset;
				/* Do not force initialization of the following pages. */
				init_page_offset = 0;
			}
			init_if_needed(ubuf);
			if (URSNeedsWAL(urs))
			{
				register_insert_page_offset_if_needed(ubuf, page_offset);

				if (input_offset == 0)
				{
					if (urs->need_type_header)
					{
						/*
						 * We'll need to create a new URS in recovery, so we
						 * capture an image of the type header.
						 */
						ubuf->bufdata.flags |= URS_XLOG_CREATE;
						ubuf->bufdata.urs_type = urs->type;
						ubuf->bufdata.type_header = urs->type_header;
						ubuf->bufdata.type_header_size = urs->type_header_size;
					}
					else
					{
						/*
						 * We'll need to add a new chunk to an existing URS in
						 * recovery.
						 */
						ubuf->bufdata.flags |= URS_XLOG_ADD_CHUNK;
						ubuf->bufdata.urs_type = urs->type;
						ubuf->bufdata.previous_chunk_header_location =
							chunk_header.previous_chunk;
					}
				}
			}
			if (page_offset == SizeOfUndoPageHeaderData)
				register_new_page(ubuf, urs->type, urs->chunk_start);
			bytes_written =
				UndoPageInsertHeader(BufferGetPage(ubuf->buffer),
									 page_offset,
									 input_offset,
									 &chunk_header,
									 type_header_size,
									 urs->type_header,
									 urs->chunk_start);
			MarkBufferDirty(ubuf->buffer);
			urs->chunks[urs->nchunks - 1].chunk_header_written = true;
			page_offset += bytes_written;
			input_offset += bytes_written;
			if (input_offset >= all_header_size)
				break;

			/* Any remaining bytes go onto the next page. */
			page_offset = SizeOfUndoPageHeaderData;
			++buffer_index;
		}
	}

	/* Write out the record. */
	input_offset = 0;
	for (;;)
	{
		UndoBuffer *ubuf = &urs->buffers[buffer_index];

		if (buffer_index >= urs->nbuffers)
			elog(ERROR, "ran out of buffers while inserting undo record");
		init_if_needed(ubuf);
		if (URSNeedsWAL(urs))
			register_insert_page_offset_if_needed(ubuf, page_offset);
		if (page_offset == SizeOfUndoPageHeaderData)
			register_new_page(ubuf, urs->type, urs->chunk_start);
		bytes_written =
			UndoPageInsertRecord(BufferGetPage(urs->buffers[buffer_index].buffer),
								 page_offset,
								 input_offset,
								 record_size,
								 record_data,
								 urs->chunk_start,
								 urs->type);
		MarkBufferDirty(urs->buffers[buffer_index].buffer);
		page_offset += bytes_written;
		input_offset += bytes_written;
		if (input_offset >= record_size)
			break;

		/* Any remaining bytes go onto the next page. */
		page_offset = SizeOfUndoPageHeaderData;
		++buffer_index;
	}

	urs->state = URS_STATE_DIRTY;

	/* Advance the insert pointer in shared memory. */
	LWLockAcquire(&urs->slot->meta_lock, LW_EXCLUSIVE);
	urs->slot->meta.insert =
		UndoLogOffsetPlusUsableBytes(urs->slot->meta.insert,
									 all_header_size + record_size);
	LWLockRelease(&urs->slot->meta_lock);

	/*
	 * If we created a new chunk, we may also need to mark the previous chunk
	 * closed.  In that case, UndoPrepareToInsert() will have pinned and
	 * locked the relevant buffers for us.
	 */
	if (urs->nchunks > 1 &&
		urs->chunks[urs->nchunks - 2].chunk_header_buffer_index[0] != -1)
	{
		UndoMarkChunkClosed(urs, &urs->chunks[urs->nchunks - 2], false);
		urs->chunks[urs->nchunks - 2].chunk_header_buffer_index[0] = -1;
	}

	/* We don't need another chunk header unless we switch undo logs. */
	urs->need_chunk_header = false;

	/* We don't ever need another type header. */
	urs->need_type_header = false;
}

/*
 * Set a boolean flag located at given offset in the chunk header (or in the
 * chunk type header) to true.
 */
void
UndoSetChunkHeaderFlag(UndoRecPtr chunk_hdr, uint16 off, Buffer buf,
					   uint8 first_block_id)
{
	UndoLogOffset chunk_hdr_off = UndoRecPtrGetOffset(chunk_hdr);
	UndoRecordSetXLogBufData bufdata;
	UndoLogOffset	flag_off;
	bool	new_value = true;
	XLogRecPtr	lsn;

	/* Find out where in the log is the flag stored ... */
	flag_off = UndoLogOffsetPlusUsableBytes(chunk_hdr_off, off);
	/* ... and record the position on the page. */
	memset(&bufdata, 0, sizeof(UndoRecordSetXLogBufData));
	bufdata.flags = URS_XLOG_SET_FLAG;
	bufdata.flag_offset = UndoRecPtrGetPageOffset(flag_off);

	UndoPageOverwrite(BufferGetPage(buf),
					  bufdata.flag_offset,
					  0,
					  sizeof(bool),
					  (char *) &new_value);
	MarkBufferDirty(buf);
	XLogRegisterBuffer(first_block_id, buf, REGBUF_KEEP_DATA);
	EncodeUndoRecordSetXLogBufData(&bufdata, first_block_id);

	/*
	 * Attach the update to a dummy WAL record.
	 *
	 * TODO Either introduce a new kind of WAL record for this purpose or
	 * rename XLOG_UNDOLOG_CLOSE_URS so it matches all the current use cases.
	 */
	lsn = XLogInsert(RM_UNDOLOG_ID, XLOG_UNDOLOG_CLOSE_URS);

	PageSetLSN(BufferGetPage(buf), lsn);
}

/*
 * Insert an undo record and/or replay other undo data modifications that were
 * performed at DO time.  If an undo record was inserted at DO time, the exact
 * same record data and size must be passed in at REDO time.  If no undo
 * record was inserted at DO time, but an URS might have been closed (thereby
 * updating a header), then pass a null pointer and zero size.
 *
 * Return a pointer to the record that was inserted, if record_data was
 * provided.
 */
UndoRecPtr
UndoReplay(XLogReaderState *xlog_record, void *record_data, size_t record_size)
{
	int			nbuffers;
	UndoLogSlot *slot;
	UndoRecPtr	result = InvalidUndoRecPtr;
	UndoBuffer *buffers;
	bool		record_more = false;
	int			record_offset = 0;
	UndoRecordSetChunkHeader chunk_header;
	bool		header_more = false;
	int			header_offset = 0;
	char	   *type_header = NULL;
	int			type_header_size = 0;
	bool		chunk_size_more = false;
	size_t		chunk_size;
	int			chunk_size_offset = 0;

	Assert(InRecovery);

	/* Make an array big enough to hold all registered blocks. */
	nbuffers = 0;
	buffers = palloc(sizeof(*buffers) * (xlog_record->max_block_id + 1));

	/* Read and lock all referenced undo log buffers. */
	for (uint8 block_id = 0; block_id <= xlog_record->max_block_id; ++block_id)
	{
		DecodedBkpBlock *block = &xlog_record->blocks[block_id];

		if (block->in_use && block->smgrid == SMGR_UNDO)
		{
			XLogRedoAction action;
			ReadBufferMode rbm;
			UndoLogOffset past_this_block;
			bool		skip = false;
			UndoRecordSetXLogBufData *bufdata = &buffers[nbuffers].bufdata;
			Page		page;
			UndoPageHeader uph;
			UndoRecPtr	chunk_start = InvalidUndoRecPtr;

			/* Figure out which undo log is referenced. */
			slot = UndoLogGetSlot(block->rnode.relNode, false);

			/*
			 * Check if we need to extend the physical range to cover this
			 * block.
			 */
			past_this_block = (block->blkno + 1) * BLCKSZ;
			if (slot->end < past_this_block)
				UndoLogAdjustPhysicalRange(slot->logno, 0, past_this_block);

			/*
			 * We could decide if it should be zeroed or not based on whether
			 * we're inserting the first byte into a page, as a kind of
			 * cross-check.  For now, we just check if a UndoInsert() marked
			 * it as needing to be initialized.
			 */
			if ((block->flags & BKPBLOCK_WILL_INIT) != 0)
			{
				rbm = RBM_ZERO_AND_LOCK;
				buffers[nbuffers].is_new = true;
				buffers[nbuffers].needs_init = true;
				buffers[nbuffers].init_page_offset = 0;
			}
			else
				rbm = RBM_NORMAL;

			/* Read the buffer. */
			action = XLogReadBufferForRedoExtended(xlog_record,
												   block_id,
												   rbm,
												   false,
												   &buffers[nbuffers].buffer);

			/*
			 * If the block was restored from a full-page image, we don't need
			 * to make any modifications, but we still need to keep track of
			 * the insertion pointer, in case an insertion spills over onto
			 * the next page.
			 *
			 * If the block was not found, then it must be discarded later in
			 * the WAL.
			 *
			 * In both of these cases, we'll just remember to skip modifying
			 * the page.
			 */
			if (action == BLK_RESTORED || action == BLK_NOTFOUND)
				skip = true;

			if (!DecodeUndoRecordSetXLogBufData(bufdata, xlog_record, block_id))
				elog(ERROR, "failed to decode undo xlog buffer data");
			page = BufferGetPage(buffers[nbuffers].buffer);
			uph = (UndoPageHeader) page;

			/*
			 * The UndoPageXXX function need the chunk start location, if they
			 * are writing to a new page.
			 */
			if (bufdata->flags & URS_XLOG_ADD_PAGE)
				chunk_start = bufdata->chunk_header_location;

			/*
			 * If there is an insertion point recorded, it must be restored
			 * before we redo (or skip) the insertion.
			 */
			if (bufdata->flags & URS_XLOG_INSERT)
			{
				if (!record_data)
					elog(ERROR, "undo buf data contained an insert page offset, but no record was passed to UndoReplay()");
				/* Update the insertion point on the page. */
				uph->ud_insertion_point = bufdata->insert_page_offset;

				/*
				 * Also update it in shared memory, though this isn't really
				 * necessary as it'll be overwritten after we write data into
				 * the page.
				 */
				slot->meta.insert =
					BLCKSZ * block->blkno + bufdata->insert_page_offset;
			}
			/*
			 * Are we still writing a chunk size that spilled into the next
			 * page?
			 */
			if (chunk_size_more)
			{
				if (skip)
				{
					chunk_size_offset += UndoPageSkipOverwrite(SizeOfUndoPageHeaderData,
															   chunk_size_offset,
															   sizeof(chunk_size));
				}
				else
				{
					chunk_size_offset += UndoPageOverwrite(page,
														   SizeOfUndoPageHeaderData,
														   chunk_size_offset,
														   sizeof(chunk_size),
														   (char *) &chunk_size);
					MarkBufferDirty(buffers[nbuffers].buffer);
				}
				Assert(chunk_size_offset == sizeof(chunk_size));
				chunk_size_more = false;
			}

			/* Are we still writing a header that spilled into the next page? */
			else if (header_more)
			{
				if (skip)
					header_offset += UndoPageSkipHeader(page,
														SizeOfUndoPageHeaderData,
														header_offset,
														type_header_size);
				else
				{
					header_offset += UndoPageInsertHeader(page,
														  SizeOfUndoPageHeaderData,
														  header_offset,
														  &chunk_header,
														  type_header_size,
														  type_header,
														  bufdata->chunk_header_location);
					MarkBufferDirty(buffers[nbuffers].buffer);
				}

				/*
				 * The shared memory insertion point must be after this
				 * fragment.
				 */

				/*
				 * TODO: consolidate the places we maintain meta.insert, fix
				 * the locking, and update shm just once at the end of the WAL
				 * record
				 */
				if (bufdata->flags & URS_XLOG_INSERT)
					slot->meta.insert = BLCKSZ * block->blkno + uph->ud_insertion_point;
				/* Do we need to go around again, on the next page? */
				if (header_offset < SizeOfUndoRecordSetChunkHeader + type_header_size)
				{
					nbuffers++;
					continue;
				}

				/* We have finished writing the header. */
				header_more = false;
			}


			/* Are we still writing a record that spilled into the next page? */
			else if (record_more)
			{
				if (skip)
					record_offset += UndoPageSkipRecord(page,
														SizeOfUndoPageHeaderData,
														record_offset,
														record_size);
				else
				{
					record_offset += UndoPageInsertRecord(page,
														  SizeOfUndoPageHeaderData,
														  record_offset,
														  record_size,
														  record_data,
														  bufdata->chunk_header_location,
														  bufdata->urs_type);
					MarkBufferDirty(buffers[nbuffers].buffer);
				}

				/*
				 * The shared memory insertion point must be after this
				 * fragment.
				 */
				if (bufdata->flags & URS_XLOG_INSERT)
					slot->meta.insert = BLCKSZ * block->blkno + uph->ud_insertion_point;

				/* Do we need to go around again, on the next page? */
				if (record_offset < record_size)
				{
					nbuffers++;
					continue;
				}

				/* We have finished writing the record. */
				record_more = false;
				record_data = NULL;
			}

			/* Check if we need to write a chunk header. */
			if (bufdata->flags & URS_XLOG_CREATE)
			{
				type_header_size = bufdata->type_header_size;

				if (skip)
				{
					header_offset = UndoPageSkipHeader(page,
													   uph->ud_insertion_point,
													   header_offset,
													   type_header_size);
				}
				else
				{
					chunk_header.size = 0;
					chunk_header.previous_chunk = InvalidUndoRecPtr;
					chunk_header.discarded = false;
					chunk_header.type = bufdata->urs_type;

					type_header = bufdata->type_header;
					header_offset = UndoPageInsertHeader(page,
														 uph->ud_insertion_point,
														 0,
														 &chunk_header,
														 type_header_size,
														 type_header,
														 chunk_start);
					MarkBufferDirty(buffers[nbuffers].buffer);
				}

				/* Do we need to go around again, on the next page? */
				if (header_offset < SizeOfUndoRecordSetChunkHeader + type_header_size)
				{
					header_more = true;
					nbuffers++;
					continue;
				}
			}

			/* Check if we need to create a new chunk for an existing URS. */
			if (bufdata->flags & URS_XLOG_ADD_CHUNK)
			{
				/* Can only be creating one chunk per WAL record. */
				Assert(!(bufdata->flags & URS_XLOG_CREATE));

				if (skip)
				{
					header_offset += UndoPageSkipHeader(page,
														uph->ud_insertion_point,
														header_offset,
														type_header_size);
				}
				else
				{
					chunk_header.size = 0;
					chunk_header.previous_chunk = bufdata->previous_chunk_header_location;
					chunk_header.discarded = false;
					chunk_header.type = bufdata->urs_type;
					type_header = NULL;
					type_header_size = 0;
					header_offset = UndoPageInsertHeader(page,
														 uph->ud_insertion_point,
														 0,
														 &chunk_header,
														 0,
														 NULL,
														 chunk_start);
					MarkBufferDirty(buffers[nbuffers].buffer);
				}

				if (header_offset < SizeOfUndoRecordSetChunkHeader)
				{
					header_more = true;
					nbuffers++;
					continue;
				}
			}

			/* Check if we need to insert the caller's record data. */
			if (record_data)
			{
				/*
				 * Remember the insert pointer, to be returned to the caller.
				 */
				Assert(result == InvalidUndoRecPtr);
				result = MakeUndoRecPtr(slot->logno,
										BLCKSZ * block->blkno + uph->ud_insertion_point);

				if (skip)
				{
					record_offset += UndoPageSkipRecord(page,
														uph->ud_insertion_point,
														record_offset,
														record_size);
				}
				else
				{
					record_offset = UndoPageInsertRecord(page,
														 uph->ud_insertion_point,
														 0,
														 record_size,
														 record_data,
														 bufdata->chunk_header_location,
														 bufdata->urs_type);
					MarkBufferDirty(buffers[nbuffers].buffer);
				}

				/*
				 * The shared memory insertion point must be after this
				 * fragment.
				 */
				if (bufdata->flags & URS_XLOG_INSERT)
					slot->meta.insert = BLCKSZ * block->blkno + uph->ud_insertion_point;

				/* Do we need to go around again, on the next page? */
				if (record_offset < record_size)
				{
					record_more = true;
					nbuffers++;
					continue;
				}
				record_data = NULL;
			}

			if (bufdata->flags & URS_XLOG_CLOSE_CHUNK)
			{
				/* Update the chunk header size to mark it closed. */
				chunk_size = bufdata->chunk_size;

				if (skip)
				{
					chunk_size_offset = UndoPageSkipOverwrite(bufdata->chunk_size_page_offset,
															  chunk_size_offset,
															  sizeof(chunk_size));
				}
				else
				{
					chunk_size_offset =
						UndoPageOverwrite(page,
										  bufdata->chunk_size_page_offset,
										  0,
										  sizeof(bufdata->chunk_size),
										  (char *) &chunk_size);
				}

				/*
				 * The shared memory insertion point must be after this
				 * fragment.
				 */
				if (bufdata->flags & URS_XLOG_INSERT)
					slot->meta.insert = BLCKSZ * block->blkno + uph->ud_insertion_point;

				/*
				 * Note on undo execution : If we closed an UndoRecordSet of
				 * type URST_TRANSACTION, the undo records will be applied
				 * according to the transaction status at the end of crash
				 * recovery.
				 */

				/*
				 * XXX IS it OK that we delivered the callback before writing
				 * the part the spills onto the next page?
				 */
				if (chunk_size_offset < sizeof(chunk_size))
				{
					chunk_size_more = true;
					nbuffers++;
					continue;
				}
			}

			/* Set boolean flag? */
			if (bufdata->flags & URS_XLOG_SET_FLAG)
			{
				/*
				 * This flag should only be used during undo execution, the
				 * caller should not try to reconstruct the undo log.
				 */
				Assert(record_data == NULL);

				if (skip)
					UndoPageSkipOverwrite(bufdata->flag_offset,
										  0, sizeof(bool));
				else
				{
					bool	new_value = true;

					UndoPageOverwrite(page,
									  bufdata->flag_offset,
									  0,
									  sizeof(new_value),
									  (char *) &new_value);
					MarkBufferDirty(buffers[nbuffers].buffer);
				}
			}

			++nbuffers;
		}
	}

	/*
	 * There had better not be any header or record data destined for the next
	 * buffer if we have run out of registered buffers.
	 */
	if (header_more || record_more)
		elog(ERROR, "undo data didn't fit on registered buffers");

	/* Update the page LSNs and release. */
	for (int i = 0; i < nbuffers; ++i)
	{
		Buffer		buffer = buffers[i].buffer;

		if (BufferIsValid(buffer))
		{
			MarkBufferDirty(buffer);
			PageSetLSN(BufferGetPage(buffer), xlog_record->ReadRecPtr);
			UnlockReleaseBuffer(buffer);
		}
	}

	pfree(buffers);

	return result;
}

/*
 * Register all undo buffers touched by a single WAL record.  This must be
 * done after an UndoInsert() and any UndoMarkClosed() calls, but before
 * calling XLogInsert().
 *
 * The caller must have called XLogBeginInsert() for a WAL record, and
 * must provide the first block ID to use, to avoid collisions with any
 * other block IDs registered by the caller.
 */
void
UndoXLogRegisterBuffers(UndoRecordSet *urs, uint8 first_block_id)
{
	if (!URSNeedsWAL(urs))
		return;

	for (int i = 0; i < urs->nbuffers; ++i)
	{
		UndoBuffer *ubuf = &urs->buffers[i];

		XLogRegisterBuffer(first_block_id + i,
						   ubuf->buffer,
						   (ubuf->is_new ? REGBUF_WILL_INIT : 0) |
						   REGBUF_UNDO);
		if (ubuf->bufdata.flags != 0)
			EncodeUndoRecordSetXLogBufData(&ubuf->bufdata,
										   first_block_id + i);
	}
}

/*
 * Set page LSNs for buffers dirtied by UndoInsert or UndoMarkClosed.
 */
void
UndoPageSetLSN(UndoRecordSet *urs, XLogRecPtr lsn)
{
	for (int i = 0; i < urs->nbuffers; ++i)
		PageSetLSN(BufferGetPage(urs->buffers[i].buffer), lsn);
}

/*
 * Release buffer locks and pins held by an UndoRecordSet.
 */
void
UndoRelease(UndoRecordSet *urs)
{
	for (int i = 0; i < urs->nbuffers; ++i)
		UnlockReleaseBuffer(urs->buffers[i].buffer);
	urs->nbuffers = 0;
}

/*
 * Destroy an UndoRecordSet.
 *
 * If any data has been written, the UndoRecordSet must be closed before it
 * is destroyed.
 */
void
UndoDestroy(UndoRecordSet *urs)
{
	/* Release buffer locks. */
	UndoRelease(urs);

	/* If you write any data, you also have to close it properly. */
	if (urs->state == URS_STATE_DIRTY)
		elog(PANIC, "dirty undo record set not closed before release");

	/* Return undo logs to appropriate free lists. */
	for (int i = 0; i < urs->nchunks; ++i)
		UndoLogRelease(urs->chunks[i].slot);

	/* Remove from list of all known record sets. */
	slist_delete(&UndoRecordSetList, &urs->link);

	/* Free memory. */
	pfree(urs->chunks);
	pfree(urs->buffers);
	pfree(urs);
}

/*
 * Reset undo insertion state.
 *
 * This code is invoked during transaction abort to forget about any buffers
 * we think we've locked in UndoAllocate() or UndoPrepareToMarkClosed(); such
 * locks have already been released, and we'll have to reacquire them to
 * close the UndoRecordSet.
 */
void
UndoResetInsertion(void)
{
	slist_iter	iter;

	slist_foreach(iter, &UndoRecordSetList)
	{
		UndoRecordSet *urs = slist_container(UndoRecordSet, link, iter.cur);

		urs->nbuffers = 0;
	}
}

/*
 * Prepare to mark UndoRecordSets for this transaction level closed.
 *
 * Like UndoPrepareToMarkClosed, this should be called prior to entering
 * a critical section.
 *
 * Returns true if there is work to be done and false otherwise; caller may
 * skip directly to UndoDestroyForXactLevel if the return value is false.
 */
bool
UndoPrepareToMarkClosedForXactLevel(int nestingLevel)
{
	slist_iter	iter;
	bool		needs_work = false;

	slist_foreach(iter, &UndoRecordSetList)
	{
		UndoRecordSet *urs = slist_container(UndoRecordSet, link, iter.cur);

		if (nestingLevel <= urs->nestingLevel &&
			urs->state == URS_STATE_DIRTY &&
			UndoPrepareToMarkClosed(urs))
			needs_work = true;
	}

	return needs_work;
}

/*
 * Mark UndoRecordSets for this transaction level closed.
 *
 * Like UndoMarkClosed, this should be called from within the critical section,
 * during WAL record construction.
 */
void
UndoMarkClosedForXactLevel(int nestingLevel)
{
	slist_iter	iter;

	slist_foreach(iter, &UndoRecordSetList)
	{
		UndoRecordSet *urs = slist_container(UndoRecordSet, link, iter.cur);

		if (nestingLevel <= urs->nestingLevel &&
			urs->state == URS_STATE_DIRTY)
			UndoMarkClosed(urs);
	}
}

/*
 * Register XLog buffers for all UndoRecordSets for this transaction level.
 *
 * This should be called from within the critical section, during WAL record
 * construction.
 */
void
UndoXLogRegisterBuffersForXactLevel(int nestingLevel, uint8 first_block_id)
{
	slist_iter	iter;

	slist_foreach(iter, &UndoRecordSetList)
	{
		UndoRecordSet *urs = slist_container(UndoRecordSet, link, iter.cur);

		if (nestingLevel <= urs->nestingLevel &&
			urs->state != URS_STATE_CLEAN)	/* TODO: can we get rid of the
											 * state test here? */
			UndoXLogRegisterBuffers(urs, first_block_id);
	}
}

/*
 * Set page LSNs for all UndoRecordSets for this transaction level.
 *
 * Like UndoPageSetLSN, this should be called just after XLogInsert.
 */
void
UndoPageSetLSNForXactLevel(int nestingLevel, XLogRecPtr lsn)
{
	slist_iter	iter;

	slist_foreach(iter, &UndoRecordSetList)
	{
		UndoRecordSet *urs = slist_container(UndoRecordSet, link, iter.cur);

		if (nestingLevel <= urs->nestingLevel &&
			urs->state == URS_STATE_DIRTY)
			UndoPageSetLSN(urs, lsn);
	}
}

/*
 * Destroy UndoRecordSets for this transaction level.
 *
 * Like UndoDestroy, this should be called after the UndoRecordSet has been
 * marked closed and the surrounding critical section has ended.
 */
void
UndoDestroyForXactLevel(int nestingLevel)
{
	slist_iter	iter;
	bool		restart = true;

	/*
	 * First, release all buffer locks.
	 *
	 * It seems like a good idea not to hold any LWLocks for longer than
	 * necessary, so do this step for every UndoRecordSet first.
	 */
	slist_foreach(iter, &UndoRecordSetList)
	{
		UndoRecordSet *urs = slist_container(UndoRecordSet, link, iter.cur);

		if (nestingLevel <= urs->nestingLevel)
			UndoRelease(urs);
	}

	/*
	 * Now destroy the UndoRecordSets.
	 *
	 * UndoDestroy will update UndoRecordSetList, so we have to restart the
	 * iterator after calling it. This might seem like an inefficient
	 * approach, but in practice the list shouldn't have more than a few
	 * elements and the ones we care about are probably all at the beginning,
	 * so it shouldn't really matter.
	 */
	while (restart)
	{
		restart = false;

		slist_foreach(iter, &UndoRecordSetList)
		{
			UndoRecordSet *urs;

			urs = slist_container(UndoRecordSet, link, iter.cur);
			if (nestingLevel <= urs->nestingLevel)
			{
				UndoDestroy(urs);
				restart = true;
				break;
			}
		}
	}
}

/*
 * Close and release all UndoRecordSets for this transaction level.
 *
 * This should normally be used only when a transaction or subtransaction ends
 * without writing some other WAL record to which the closure of the
 * UndoRecordSet could be attached. Special kind of WAL record is used to
 * carry the buffer metadata.
 *
 * Returns true if we did anything, and false if nothing needed to be done.
 */
bool
UndoCloseAndDestroyForXactLevel(int nestingLevel)
{
	XLogRecPtr	lsn;
	bool		needs_work;

	needs_work = UndoPrepareToMarkClosedForXactLevel(nestingLevel);

	if (needs_work)
	{
		xl_undolog_close_urs xlrec;

		START_CRIT_SECTION();
		XLogBeginInsert();
		UndoMarkClosedForXactLevel(nestingLevel);
		UndoXLogRegisterBuffersForXactLevel(nestingLevel, 0);
		XLogRegisterData((char *) &xlrec, sizeof(xlrec));
		lsn = XLogInsert(RM_UNDOLOG_ID, XLOG_UNDOLOG_CLOSE_URS);
		UndoPageSetLSNForXactLevel(nestingLevel, lsn);
		END_CRIT_SECTION();
	}

	UndoDestroyForXactLevel(nestingLevel);

	return needs_work;
}

/*
 * Find the start of the final chunk by examining a page that is known to be
 * the final page in an undo log (ie holding the byte that precedes the
 * insertion point).
 */
static UndoRecPtr
find_start_of_chunk_on_final_page(Page page, UndoRecPtr page_begin_urp)
{
	UndoPageHeader page_header = (UndoPageHeader) page;
	UndoLogOffset size;

	/*
	 * We'll access the initial size member of chunk headers directly, so
	 * let's assert that the layout is as this code expects.
	 */
	Assert(offsetof(UndoRecordSetChunkHeader, size) == 0);
	Assert(sizeof(((UndoRecordSetChunkHeader *) 0)->size) == sizeof(size));

	/* Search for the start of the final chunk on this page. */
	if (page_header->ud_first_chunk > 0)
	{
		uint16		page_offset = page_header->ud_first_chunk;

		/* Walk forwards until we find the last chunk on the page. */
		for (;;)
		{
			/*
			 * The size must be entirely on this page, or this wouldn't be the
			 * last page in the log.
			 */
			if (page_offset > BLCKSZ - sizeof(size))
				elog(ERROR, "unexpectedly ran out of undo page while reading chunk size");

			/* Read the aligned value. */
			memcpy(&size, page + page_offset, sizeof(size));

			/*
			 * The chunk can't spill onto the next page, or this wouldn't be
			 * the last page in the log.
			 */
			if (page_offset + size > BLCKSZ)
				elog(ERROR, "unexpectedly ran out of undo page while following chunks");

			/* The chunk can't extend past the insertion point. */
			if (page_offset + size > page_header->ud_insertion_point)
				elog(ERROR, "undo chunk exceeded expected range");

			/*
			 * The last chunk is the one that either hits the insertion point
			 * or is has size zero (unclosed).
			 */
			if (size == 0 || page_offset + size == page_header->ud_insertion_point)
				return page_begin_urp + page_offset;

			/* Keep walking. */
			page_offset += size;
		}
		return InvalidUndoRecPtr;	/* unreachable */
	}
	else
	{
		/*
		 * If no chunks have been started on the page, then the start of the
		 * chunk that spilled into this page is directly available from the
		 * header.
		 */
		return page_header->ud_continue_chunk;
	}
}

/*
 * Given the current insert pointer for an undo log, find the header for the
 * last chunk.  That is, the chunk that contains the byte that immediately
 * precedes the insert pointer.  The discard pointer must be before the insert
 * pointer (ie there must actually be an undiscared byte immediately preceding
 * the insert pointer).
 */
static UndoRecPtr
find_start_of_final_chunk_in_undo_log(UndoLogNumber logno, UndoLogOffset insert)
{
	RelFileNode rnode;
	UndoLogOffset last_data_offset;
	UndoRecPtr	final_page_urp;
	UndoRecPtr	result;
	BlockNumber chunk_last_blockno;
	Buffer		buffer;

	/*
	 * Locate the page holding the byte preceding the insert point, skipping
	 * over the page header if necessary, because that's the last page that
	 * had anything written to it and thus that has the page header
	 * information we need to find our way.
	 */
	last_data_offset = insert - 1;
	if (last_data_offset % BLCKSZ < SizeOfUndoPageHeaderData)
		last_data_offset -= SizeOfUndoPageHeaderData;

	/* Read the last chunk location from the last page's header. */
	UndoRecPtrAssignRelFileNode(rnode, MakeUndoRecPtr(logno, last_data_offset));
	chunk_last_blockno = last_data_offset / BLCKSZ;
	buffer = ReadBufferWithoutRelcache(SMGR_UNDO,
									   rnode,
									   MAIN_FORKNUM,
									   chunk_last_blockno,
									   RBM_NORMAL,
									   NULL,
									   RELPERSISTENCE_PERMANENT);

	/*
	 * Cannot do anything if the interesting part of undo log was deleted (or
	 * discarded?) accidentally.
	 */
	if (!BufferIsValid(buffer))
		ereport(ERROR, (errmsg("could not read undo log buffer")));

	LockBuffer(buffer, BUFFER_LOCK_SHARE);

	/* Find the start of the final chunk by examining this page. */
	final_page_urp = MakeUndoRecPtr(logno, BLCKSZ * chunk_last_blockno);
	result = find_start_of_chunk_on_final_page(BufferGetPage(buffer),
											   final_page_urp);
	UnlockReleaseBuffer(buffer);

	return result;
}

/*
 * Read data at a given location, reading and locking buffers as required.
 * This is a helper function for CloseDanglingUndoRecordSets().
 */
static void
read_undo_header(char *out, size_t size, UndoRecPtr urp,
				 Buffer *buffers, int nbuffers)
{
	Buffer		buffer;
	RelFileNode rnode;
	BlockNumber blockno;
	uint16		page_offset;
	uint16		bytes_on_this_page;
	size_t		bytes_copied;
	int			buffer_index = 0;

	UndoRecPtrAssignRelFileNode(rnode, urp);
	blockno = UndoRecPtrGetOffset(urp) / BLCKSZ;
	page_offset = urp % BLCKSZ;
	bytes_copied = 0;

	do
	{
		if (buffer_index >= nbuffers)
			elog(ERROR, "cannot read undo data: not enough buffers");
		buffer = ReadBufferWithoutRelcache(SMGR_UNDO,
										   rnode,
										   MAIN_FORKNUM,
										   blockno,
										   RBM_NORMAL,
										   NULL,
										   RELPERSISTENCE_PERMANENT);
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		buffers[buffer_index] = buffer;
		bytes_on_this_page = Min(size - bytes_copied, BLCKSZ - page_offset);
		memcpy((char *) out + bytes_copied,
			   BufferGetPage(buffer) + page_offset,
			   bytes_on_this_page);
		bytes_copied += bytes_on_this_page;
		blockno++;
		buffer_index++;
		page_offset = SizeOfUndoPageHeaderData;
	}
	while (bytes_copied < size);
}

static void
release_buffers(Buffer *buffers, int nbuffers)
{
	for (int i = 0; i < nbuffers; ++i)
	{
		if (buffers[i] != InvalidBuffer)
		{
			UnlockReleaseBuffer(buffers[i]);
			buffers[i] = InvalidBuffer;
		}
	}
}

/*
 * Scan the set of existing undo logs looking for URS chunks that are not
 * closed (ie that have a zero length header).  This is done to discover URSs
 * that were open at the time of a crash, at startup.  We'll set the chunk
 * length so that we know how to discard it, and we'll call the URS
 * type-specific callback to tell it we're closing one of its URSs that was
 * found to be dangling after a crash.
 */
void
CloseDanglingUndoRecordSets(void)
{
	UndoLogSlot *slot = NULL;

	while ((slot = UndoLogGetNextSlot(slot)))
	{
		UndoLogNumber logno = slot->logno;
		UndoLogOffset discard = slot->meta.discard;
		UndoLogOffset insert = slot->meta.insert;
		UndoRecPtr	chunk_header_location;
		UndoRecPtr	begin;
		UndoRecordSetChunkHeader chunk_header;
		UndoRecordSetType type;
		Buffer		buffers[2];
		void	   *type_header;
		size_t		type_header_size;
		size_t		chunk_size;
		uint16		page_offset;
		uint16		bytes_on_first_page;
		UndoRecordSetXLogBufData bufdata = {0};
		xl_undolog_close_urs xlrec;
		XLogRecPtr	lsn;

		/* If the undo is empty, skip. */
		if (insert == discard)
			continue;

		/* Locate the header of the final chunk. */
		Assert(discard < insert);
		chunk_header_location =
			find_start_of_final_chunk_in_undo_log(logno, insert);

		for (int i = 0; i < lengthof(buffers); ++i)
			buffers[i] = InvalidBuffer;

		/* Read the chunk header. */
		read_undo_header((void *) &chunk_header, SizeOfUndoRecordSetChunkHeader,
						 chunk_header_location,
						 buffers, lengthof(buffers));
		release_buffers(buffers, lengthof(buffers));

		/*
		 * We already released the buffer(s) because it makes the later code a
		 * bit simpler, and it's also the expected outcome that we won't need
		 * them.  A non-zero size indicates that there is nothing to be done
		 * here, the chunk was already closed and we can move onto the next
		 * undo log.
		 */
		if (chunk_header.size > 0)
			continue;

		/* Compute the missing chunk size. */
		Assert(insert > UndoRecPtrGetOffset(chunk_header_location));
		chunk_size = insert - UndoRecPtrGetOffset(chunk_header_location);

		/*
		 * In order to log it, we need to get our hands on the location of the
		 * first chunk in this URS, and the type-specific header that follows
		 * its header.  Walk back to the start of the chain, if we aren't
		 * already there.
		 *
		 * XXX Be smarter about not releasing and reacquiring the buffers?
		 */
		begin = chunk_header_location;
		while (chunk_header.previous_chunk != InvalidUndoRecPtr)
		{
			begin = chunk_header.previous_chunk;
			/* XXX think harder about this case --- just skip it? */
			if (UndoRecPtrIsDiscarded(begin))
				elog(PANIC, "found partially discarded unclosed undo record set");
			read_undo_header((void *) &chunk_header, SizeOfUndoRecordSetChunkHeader,
							 begin,
							 buffers, lengthof(buffers));
			release_buffers(buffers, lengthof(buffers));
		}
		type = chunk_header.type;
		type_header_size = get_urs_type_header_size(type);
		type_header = palloc(type_header_size);
		read_undo_header(type_header, type_header_size,
						 UndoRecPtrPlusUsableBytes(begin, SizeOfUndoRecordSetChunkHeader),
						 buffers, lengthof(buffers));
		release_buffers(buffers, lengthof(buffers));

		/* Prepare to write the final chunk's missing size. */
		read_undo_header((void *) &chunk_header, SizeOfUndoRecordSetChunkHeader,
						 chunk_header_location,
						 buffers, lengthof(buffers));
		page_offset = chunk_header_location % BLCKSZ;
		bytes_on_first_page = Min(BLCKSZ - page_offset, sizeof(chunk_size));

		/* Write the final chunk's missing size and log it. */
		START_CRIT_SECTION();
		XLogBeginInsert();
		UndoPageOverwrite(BufferGetPage(buffers[0]),
						  page_offset,
						  0,
						  sizeof(chunk_size),
						  (char *) &chunk_size);
		MarkBufferDirty(buffers[0]);
		XLogRegisterBuffer(0, buffers[0], REGBUF_UNDO);
		/* We're closing a chunk and a URS. */
		bufdata.flags = URS_XLOG_CLOSE_CHUNK | URS_XLOG_CLOSE;
		bufdata.chunk_size_page_offset = page_offset;
		bufdata.chunk_size = chunk_size;

		/*
		 * We log a copy of the type-specific header to pass to
		 * report_closed_urs() in recovery, because it can't safely read it
		 * from the URS's first chunk header.
		 */
		bufdata.urs_type = type;
		bufdata.type_header = type_header;
		bufdata.type_header_size = type_header_size;
		EncodeUndoRecordSetXLogBufData(&bufdata, 0);
		if (bytes_on_first_page < sizeof(chunk_size))
		{
			UndoPageOverwrite(BufferGetPage(buffers[1]),
							  SizeOfUndoPageHeaderData,
							  bytes_on_first_page,
							  sizeof(chunk_size),
							  (char *) &chunk_size);
			MarkBufferDirty(buffers[1]);
			XLogRegisterBuffer(1, buffers[1], REGBUF_UNDO);
		}

		XLogRegisterData((char *) &xlrec, sizeof(xlrec));
		lsn = XLogInsert(RM_UNDOLOG_ID, XLOG_UNDOLOG_CLOSE_URS);
		PageSetLSN(BufferGetPage(buffers[0]), lsn);
		if (buffers[1] != InvalidBuffer)
			PageSetLSN(BufferGetPage(buffers[1]), lsn);
		END_CRIT_SECTION();

		release_buffers(buffers, lengthof(buffers));

		pfree(type_header);
	}
}

static int
logno_comparator(const void *arg1, const void *arg2)
{
	UndoLogNumber logno1 = *(const UndoLogNumber *) arg1;
	UndoLogNumber logno2 = *(const UndoLogNumber *) arg2;

	if (logno1 > logno2)
		return 1;
	if (logno1 < logno2)
		return -1;
	return 0;
}

/*
 * Following is a hash table used to merge chunks into undo record sets.
 */
typedef struct URSEntry
{
	UndoRecPtr	last_chunk;		/* pointer to the last chunk */
	UndoRecPtr	begin;			/* where the URS starts */
	UndoRecPtr	end;			/* and where it ends */

	/*
	 * The following fields are only defined for urs_type == URST_TRANSACTION.
	 */
	XactUndoRecordSetHeader xact_hdr;	/* transaction info */
	bool		applied;		/* was the URS applied? */
	bool		discarded;		/* are all chunks of this URS discarded? */
	uint8		urs_type;		/* see UndoRecordSetType */
	char		persistence;	/* relation persistence */
	char		status;			/* used by simplehash */
} URSEntry;

#define SH_PREFIX chunktable
#define SH_ELEMENT_TYPE URSEntry
#define SH_KEY_TYPE UndoRecPtr
#define SH_KEY last_chunk
#define SH_HASH_KEY(tb, key) (key)
#define SH_EQUAL(tb, a, b) ((a) == (b))
#define SH_SCOPE static inline
#define SH_DECLARE
#define SH_DEFINE
#include "lib/simplehash.h"

/*
 * Gather information on existing undo record sets, which might be split into
 * chunks.
 *
 * The problem is that only the first chunk of an undo record set contains XID
 * (which is needed to decide whether the set should be applied or discarded),
 * but it does not contain pointer to the next chunk. It's more efficient to
 * collect information on all the sets at once than to look for the non-first
 * chunks whenever we realize that particular set should be applied /
 * discarded.
 *
 * Hash table that we use to collect the information is returned.
 *
 * TODO Implement this in a way that allows the chunk information to be
 * spilled to disk. Otherwise insufficient memory will break discarding
 * altogether.
 */
static chunktable_hash *
GetAllUndoRecordSets(void)
{
	UndoLogSlot *slot = NULL;
	List	   *lognos_list = NIL;
	int		   *lognos;
	int			nlogs,
				i;
	ListCell   *lc;
	Buffer		buffers[2];
	chunktable_hash *chunks;
	URSEntry *entry;

	/*
	 * When merging the chunks into sets, we assume that the chunks at lower
	 * addresses are followed by those at higher addresses. Therefore we need
	 * to scan the slots in the log number order.
	 */
	while ((slot = UndoLogGetNextSlot(slot)))
		lognos_list = lappend_int(lognos_list, slot->logno);
	nlogs = list_length(lognos_list);
	lognos = (int *) palloc(nlogs * sizeof(int));
	i = 0;
	foreach(lc, lognos_list)
		lognos[i++] = lfirst_int(lc);
	qsort(lognos, nlogs, sizeof(int), logno_comparator);

	chunks = chunktable_create(CurrentMemoryContext, 8, NULL);

	for (i = 0; i < lengthof(buffers); ++i)
		buffers[i] = InvalidBuffer;

	for (i = 0; i < nlogs; i++)
	{
		UndoLogNumber logno;
		UndoLogOffset discard;
		UndoLogOffset insert;
		UndoLogOffset cur;
		Size		size = 0;
		char		persistence;

		slot = UndoLogGetSlot(lognos[i], false);

		/*
		 * Although callers do not expect concurrent discarding, the insert
		 * position can change concurrently, so lock the meta data to get a
		 * consistent value.
		 */
		LWLockAcquire(&slot->meta_lock, LW_SHARED);
		logno = slot->logno;
		discard = slot->meta.discard;
		insert = slot->meta.insert;
		persistence = slot->meta.persistence;
		LWLockRelease(&slot->meta_lock);

		/* If the undo is empty, skip. */
		if (insert == discard)
			continue;
		Assert(discard < insert);

		cur = discard;

		/* Scan this log. */
		while (cur < insert)
		{
			UndoRecPtr	chunk_start;
			bool		found;
			UndoRecPtr	begin;
			UndoRecordSetChunkHeader chunk_hdr;
			XactUndoRecordSetHeader xact_hdr;
			UndoRecordSetType urs_type;
			bool	discarded;
			bool	applied = false;

			chunk_start = MakeUndoRecPtr(logno, cur);

			read_undo_header((void *) &chunk_hdr,
							 SizeOfUndoRecordSetChunkHeader,
							 chunk_start,
							 buffers,
							 lengthof(buffers));
			release_buffers(buffers, lengthof(buffers));

			cur += chunk_hdr.size;
			size = chunk_hdr.size;

			/* No chunk should end beyond the insert position. */
			Assert(cur <= insert);

			/*
			 * Callers of this function are not interested in open chunks.
			 *
			 * Note: this should only be possible if the inserting backend
			 * crashed. On the other hand, if the URS is still being inserted,
			 * all its buffers should be locked in exclusive mode so we cannot
			 * include it in our result.
			 */
			if (size == 0)
			{
				/* Caller is not interested in incomplete URS as well. */
				if (chunk_hdr.previous_chunk != InvalidUndoRecPtr)
				{
					entry = chunktable_lookup(chunks,
											  chunk_hdr.previous_chunk);

					/* We should have seen the previous chunk already. */
					Assert(entry != NULL);
					Assert(entry->last_chunk == chunk_hdr.previous_chunk);

					chunktable_delete(chunks, chunk_hdr.previous_chunk);
				}

				break;
			}

			/*
			 * TODO Can empty sets be there? If we want to skip them, consider
			 * that UsableBytesPerUndoPage needs to be taken into account when
			 * checking the minimum size (i.e. the total size of headers).
			 */

			/* The first chunk is followed by the type header. */
			if (chunk_hdr.type == URST_TRANSACTION &&
				chunk_hdr.previous_chunk == InvalidUndoRecPtr)
			{
				/* TODO Shared lock on the buffers should be enough. */
				read_undo_header((void *) &xact_hdr,
								 SizeOfXactUndoRecordSetHeader,
								 UndoRecPtrPlusUsableBytes(chunk_start,
														   SizeOfUndoRecordSetChunkHeader),
								 buffers,
								 lengthof(buffers));
				release_buffers(buffers, lengthof(buffers));
			}

			/*
			 * Process the current chunk if we have one.
			 *
			 * In order to conserve memory, try to merge the chunk with an
			 * undo record set whose other chunk(s) we've already processed.
			 */
			if (chunk_hdr.previous_chunk != InvalidUndoRecPtr)
			{
				entry = chunktable_lookup(chunks,
										  chunk_hdr.previous_chunk);

				/* We should have seen the previous chunk already. */
				Assert(entry != NULL);
				Assert(entry->last_chunk == chunk_hdr.previous_chunk);

				/*
				 * Retrieve the useful information before deleting the entry.
				 */
				begin = entry->begin;
				urs_type = entry->urs_type;

				/*
				 * All chunks should be discarded or none. If a mismatch is
				 * encountered, another backend is probably discarding the URS
				 * concurrently.
				 */
				if (chunk_hdr.discarded != entry->discarded)
					elog(ERROR,
						 "some chunks of undo record set are discarded while other are not");

				discarded = entry->discarded;

				/*
				 * Non-first chunk does not have the transaction header, so
				 * only propagate the value from the earlier (and eventually
				 * the first) chunk.
				 */
				applied = entry->applied;

				/*
				 * The entry will be replaced with one for the current chunk.
				 */
				chunktable_delete(chunks, chunk_hdr.previous_chunk);
			}
			else
			{
				/* The first chunk of the URS. */
				begin = chunk_start;
				urs_type = chunk_hdr.type;
				discarded = chunk_hdr.discarded;

				if (chunk_hdr.type == URST_TRANSACTION)
					applied = xact_hdr.applied;
			}

			/*
			 * Insert a new entry that takes the previous one into account.
			 */
			entry = chunktable_insert(chunks, chunk_start, &found);
			Assert(!found);
			entry->begin = begin;
			entry->end = chunk_start + size;
			entry->urs_type = urs_type;
			if (entry->urs_type == URST_TRANSACTION)
				entry->xact_hdr = xact_hdr;
			entry->discarded = discarded;
			entry->applied = applied;
			entry->persistence = persistence;
		}
	}

	return chunks;
}

/*
 * Set a boolean flag in the chunks of the undo record set whose last chunk
 * pointer is passed.
 *
 * "off" is the position of the flag in the chunk.
 */
void
UndoSetFlag(UndoRecPtr last_chunk, uint16 off, char persistence)
{
	while (last_chunk != InvalidUndoRecPtr)
	{
		Buffer		buffers[2];
		Buffer		buffer;
		UndoRecordSetChunkHeader hdr;
		UndoRecPtr	previous_chunk;

		/*
		 * Read the chunk header in order to retrieve location of the previous
		 * chunk. Two buffers are needed in case the header crosses page
		 * boundary.
		 */
		buffers[1] = InvalidBuffer;
		read_undo_header((void *) &hdr,
						 SizeOfUndoRecordSetChunkHeader,
						 last_chunk,
						 buffers, 2);

		/* Unlock the buffer(s) but keep pin. */
		LockBuffer(buffers[0], BUFFER_LOCK_UNLOCK);
		if (buffers[1] != InvalidBuffer)
			LockBuffer(buffers[1], BUFFER_LOCK_UNLOCK);

		previous_chunk = hdr.previous_chunk;

		/* Find and lock the buffer to be modified. */
		UndoPrepareToSetChunkHeaderFlag(last_chunk, off, persistence,
										&buffer);

		START_CRIT_SECTION();

		XLogBeginInsert();

		/*
		 * Store the new value of the "discarded" field in the chunk header. A
		 * separate buffer variable is used because we don't know which buffer
		 * of 'buffers' we'll modify.
		 */
		UndoSetChunkHeaderFlag(last_chunk, off, buffer, 0);

		END_CRIT_SECTION();

		UnlockReleaseBuffer(buffer);

		/* Discard the pins we have from the reader. */
		ReleaseBuffer(buffers[0]);
		if (buffers[1] != InvalidBuffer)
			ReleaseBuffer(buffers[1]);

		/* Go for the chunk ahead of the current one. */
		last_chunk = previous_chunk;
	}
}

/*
 * Find record sets whose transaction either aborted or could not finish due
 * to server crash (these might include sets closed earlier by
 * CloseDanglingUndoRecordSets()) and execute their undo records.
 */
void
ApplyPendingUndo(void)
{
	ResourceOwner	res_owner_tmp;
	chunktable_hash *sets;
	chunktable_iterator iterator;
	URSEntry *entry;

	/*
	 * At this point we do not need a regular transaction, but need to have
	 * the CurrentResourceOwner set, in order to access the undo log in shared
	 * buffers. (It few used a transaction here, we'd also have to copy the
	 * result from that transaction's memory context.)
	 */
	Assert(CurrentResourceOwner == NULL);
	CurrentResourceOwner = ResourceOwnerCreate(CurrentResourceOwner,
											   "GetAllUndoRecordSets");
	sets = GetAllUndoRecordSets();
	res_owner_tmp = CurrentResourceOwner;
	CurrentResourceOwner = NULL;
	ResourceOwnerDelete(res_owner_tmp);

	if (sets->members == 0)
		return;

	/*
	 * Now that we have the whole record sets, check if their undo records
	 * need to be executed, and execute them.
	 *
	 * TODO If there can be multiple per-subtransaction URSs (i.e. they have
	 * the same XID) make sure they are processed in the correct order.
	 */
	chunktable_start_iterate(sets, &iterator);
	while ((entry = chunktable_iterate(sets, &iterator)) != NULL)
	{
		TransactionId xid;

		Assert(entry->urs_type == URST_TRANSACTION);

		/* The set can be discarded but the underlying log still there. */
		if (entry->discarded)
			continue;

		/* We can only process undo of the database we are connected to. */
		if (entry->xact_hdr.dboid != MyDatabaseId)
			continue;

		xid = XidFromFullTransactionId(entry->xact_hdr.fxid);

		/* URST_TRANSACTION header should always contain the XID. */
		Assert(TransactionIdIsValid(xid));

		if (TransactionIdDidCommit(xid))
		{
			/*
			 * Nothing needs to be undone. If the transaction has aborted
			 * subtransactions, those should have been undone before the
			 * commit.
			 *
			 * The record set of this transaction cannot be marked as
			 * discarded yet because while some transactions need the
			 * committed information, other transactions (in particular, the
			 * prepared transactions, as these do survive the crash) might
			 * still need the contents of the undo log (e.g. the previous
			 * tuple version if the zheap AM is concerned).
			 */
		}
		else if (TransactionIdIsPrepared(xid))
		{
			/* Prepared transactions may still need the undo log. */
		}
		else
		{
			Assert(!TransactionIdIsInProgress(xid));

			/*
			 * Transaction rolled back explicitly or failed to complete due to
			 * server crash. Execute the undo records so that the set is
			 * longer needed.
			 */
			if (!entry->applied)
			{
				/* Do the actual work. */
				PerformBackgroundUndo(entry->begin, entry->end,
									  UndoPersistenceForRelPersistence(entry->persistence));

				/*
				 * Mark the URS applied (only the first chunk can carry this
				 * information).
				 */
				if (entry->urs_type == URST_TRANSACTION)
				{
					uint16	off;

					/* Compute the offset of the "applied" flag in the chunk. */
					off = SizeOfUndoRecordSetChunkHeader +
						offsetof(XactUndoRecordSetHeader, applied);

					/*
					 * Adjust the chunk(s). Transaction is not needed here,
					 * but resource owner is.
					 */
					Assert(CurrentResourceOwner == NULL);
					CurrentResourceOwner = ResourceOwnerCreate(CurrentResourceOwner,
															   "UndoSetFlag");
					UndoSetFlag(entry->begin, off, entry->persistence);
					res_owner_tmp = CurrentResourceOwner;
					CurrentResourceOwner = NULL;
					ResourceOwnerDelete(res_owner_tmp);
				}
			}
		}
	}

	/* Cleanup. */
	chunktable_destroy(sets);
}

/*
 * It should be impossible to reach this code with any UndoRecordSet
 * still in existence, but maybe there's someway for it to happen if
 * we experience failures while trying to abort the active transaction.
 *
 * It could also happen if somebody writes code that invokes UndoCreate()
 * and doesn't provide a mechanism to make sure that the UndoRecordSet
 * gets closed.
 *
 * If it does happen, use PANIC to recover. System restart will set
 * the size of any UndoRecordSet that was not properly closed. (We could
 * also try again here, but it's not clear whether all of the services
 * that we'd need in order to do so are still working. Also, if it already
 * failed during transaction abort, it doesn't seem all that likely to
 * work now.)
 */
void
AtProcExit_UndoRecordSet(void)
{
	if (!slist_is_empty(&UndoRecordSetList))
		elog(PANIC, "undo record set not closed before backend exit");
}

/*
 * Try to advance oldestFullXidHavingUndo and return the new value. While
 * doing so, discard the URS chunks that can be discarded.
 */
TransactionId
AdvanceOldestXidHavingUndo(void)
{
	FullTransactionId oldestFullXidHavingUndo;
	TransactionId oldestXmin,
				oldestXidHavingUndo;
	TransactionId xid_orig;
	uint32		xid_orig_epoch;
	chunktable_hash *sets;
	chunktable_iterator iterator;
	URSEntry *entry;

	/*
	 * Since this function assists in undo discarding, do nothing while the
	 * undo log might still be subject to recovery.
	 *
	 * ERROR makes more sense than returning an invalid XID.
	 */
	if (RecoveryInProgress())
		ereport(ERROR,
				(errmsg("cannot advance oldestXidHavingUndo during recovery")));

	/*
	 * Although VACUUM does not need the undo of other backends for visibility
	 * checks (if it did, we'd have to track the "oldest XID having undo" in
	 * the way we track "frozen XID" for tables and databases), it does
	 * produce undo records (UNDO_ZHEAP_ITEMID_UNUSED). Therefore we must not
	 * ignore lazy VACUUM processes when determining the XID limit for undo
	 * removal.
	 */
	oldestXmin = GetOldestTransactionIdConsideredRunning();

	Assert(TransactionIdIsNormal(oldestXmin));

	/*
	 * Start with the optimistic assumption that no transaction preceding
	 * oldestXmin has undo.
	 */
	oldestXidHavingUndo = oldestXmin;

	/* Gather information on all undo record sets. */
	sets = GetAllUndoRecordSets();

	chunktable_start_iterate(sets, &iterator);
	while ((entry = chunktable_iterate(sets, &iterator)) != NULL)
	{
		TransactionId xid;

		Assert(entry->urs_type == URST_TRANSACTION);

		xid = XidFromFullTransactionId(entry->xact_hdr.fxid);

		/* Processed by the previous run? */
		if (entry->discarded)
			continue;

		/*
		 * Skip transactions that might still be visible to other
		 * transactions
		 . */
		if (!TransactionIdPrecedes(xid, oldestXmin))
			continue;

		Assert(!TransactionIdIsInProgress(xid));

		if (TransactionIdDidCommit(xid))
		{
			/* Undo should never be needed, so mark the chunk discarded. */
			UndoSetFlag(entry->last_chunk,
						offsetof(UndoRecordSetChunkHeader, discarded),
						entry->persistence);
		}
		else
		{
			/*
			 * If the transaction aborted, we can only discard the chunk(s) if
			 * the URS has already been applied. We must not try to apply the
			 * undo ourselves because 1) the backend/worker that ran the
			 * transaction might try to do itself and there's no easy way to
			 * find out (and interlock), 2) the URS might belong to a prepared
			 * transaction which can eventually commit.
			 */
			if (entry->applied)
				UndoSetFlag(entry->last_chunk,
							offsetof(UndoRecordSetChunkHeader, discarded),
							entry->persistence);
			else
			{
				/*
				 * This URS must be preserved, so adjust the discarding
				 * horizon if needed.
				 *
				 * Note that besides prepared transactions, we can see here an
				 * URS of a transaction that could not complete due to server
				 * crash. This is not too likely because the undo worker
				 * should start processing such transactions immediately after
				 * the completion of crash recovery. If we hit particular xid
				 * before the undo worker did, the next run of this function
				 * should find it as applied.
				 */
				if (TransactionIdPrecedes(xid, oldestXidHavingUndo))
					oldestXidHavingUndo = xid;
			}
		}
	}

	/* cleanup */
	chunktable_destroy(sets);

	oldestFullXidHavingUndo.value =
		pg_atomic_read_u64(&ProcGlobal->oldestFullXidHavingUndo);
	xid_orig = XidFromFullTransactionId(oldestFullXidHavingUndo);
	xid_orig_epoch = EpochFromFullTransactionId(oldestFullXidHavingUndo);

	/*
	 * Adjust the epoch if the 32-bit value has overflown. Note that it cannot
	 * happen twice between calls of this function, because at latest when
	 * oldestXidHavingUndo lags by 2 billions XIDs behind the newest active
	 * XID, it will block CLOG truncation and therefore GetNewTransactionId()
	 * will stop generating new XIDs.
	 */
	Assert(TransactionIdIsNormal(oldestXidHavingUndo));
	if (oldestXidHavingUndo < xid_orig)
		xid_orig_epoch++;

	/* Update the value in the shared memory. */
	oldestFullXidHavingUndo = FullTransactionIdFromEpochAndXid(xid_orig_epoch,
															   oldestXidHavingUndo);
	pg_atomic_write_u64(&ProcGlobal->oldestFullXidHavingUndo,
						U64FromFullTransactionId(oldestFullXidHavingUndo));

	elog(DEBUG1, "the current value of oldestXidHavingUndo is %u",
		 oldestXidHavingUndo);

	return oldestXidHavingUndo;
}

/*
 * In each log, move the discard pointer behind the last discarded undo record
 * set chunk.
 */
void
DiscardUndoRecordSetChunks(void)
{
	UndoLogSlot *slot = NULL;
	UndoRecordSetChunkHeader chunk_hdr;
	Buffer		buffers[2];
	int			i;

	/*
	 * Since AdvanceOldestXidHavingUndo() does nothing in the recovery mode,
	 * we should not find much work to do in recovery as well.  Furthermore,
	 * we don't want to perform discarding on standby cluster in other way
	 * than by replaying WAL.
	 *
	 * ERROR seems more suitable than returning because this function can be
	 * called interactively from a backend.
	 */
	if (RecoveryInProgress())
		ereport(ERROR,
				(errmsg("cannot discard undo log during recovery")));

	for (i = 0; i < lengthof(buffers); ++i)
		buffers[i] = InvalidBuffer;

	while ((slot = UndoLogGetNextSlot(slot)))
	{
		UndoLogNumber logno;
		UndoLogOffset discard;
		UndoLogOffset insert;
		UndoLogOffset cur;

		LWLockAcquire(&slot->meta_lock, LW_SHARED);
		logno = slot->logno;
		discard = slot->meta.discard;
		insert = slot->meta.insert;
		LWLockRelease(&slot->meta_lock);

		Assert(discard <= insert);

		cur = discard;
		while (cur < insert)
		{
			/*
			 * We only discard the whole chunks, so the first chunk should
			 * start exactly at the current discard position.
			 */
			read_undo_header((void *) &chunk_hdr,
							 SizeOfUndoRecordSetChunkHeader,
							 MakeUndoRecPtr(logno, cur),
							 buffers,
							 lengthof(buffers));
			release_buffers(buffers, lengthof(buffers));

			/*
			 * Stop as soon as we see an open chunk or a chunk not marked for
			 * discarding.
			 */
			if (!chunk_hdr.discarded)
				break;

			cur += chunk_hdr.size;
		}
		/* The insert pointer should also be at chunk boundary. */
		Assert(cur <= insert);

		/* Perform the actual discarding. */
		if (cur > discard)
			UndoDiscard(MakeUndoRecPtr(logno, cur));
	}
}

/*
 * User interface for AdvanceOldextXidHavingUndo(), for debugging purposes
 * only. CAUTION: It's not recommended to call this function while the discard
 * worker is running - should it be moved to an extension?
 */
Datum
pg_advance_oldest_xid_having_undo(PG_FUNCTION_ARGS)
{
	TransactionId xid = AdvanceOldestXidHavingUndo();

	PG_RETURN_TRANSACTIONID(xid);
}

/*
 * User interface for DiscardUndoRecordSetChunks(), for debugging purposes
 * only. CAUTION: It's not recommended to call this function while the discard
 * worker is running - should it be moved to an extension?
 */
Datum
pg_discard_undo_record_set_chunks(PG_FUNCTION_ARGS)
{
	DiscardUndoRecordSetChunks();

	PG_RETURN_VOID();
}


/*
 * For given persistence level, return the pointer to the beginning of the URS
 * owned by the current top-level transaction. Return NULL if there's no
 * active URS.
 */
Datum
pg_current_urs_begin(PG_FUNCTION_ARGS)
{
	UndoRecPtr	begin, end;
	char		relpersist = PG_GETARG_CHAR(0);
	UndoPersistenceLevel	p;
	char	*res_str;

	p = GetUndoPersistenceLevel(relpersist);

	if (!GetCurrentUndoRange(&begin, &end, p))
		PG_RETURN_NULL();

	res_str = psprintf(UndoRecPtrFormat, begin);
	PG_RETURN_DATUM(CStringGetTextDatum(res_str));
}

/* Like above, but return the end position. */
Datum
pg_current_urs_end(PG_FUNCTION_ARGS)
{
	UndoRecPtr	begin, end;
	char		relpersist = PG_GETARG_CHAR(0);
	UndoPersistenceLevel	p;
	char	*res_str;

	p = GetUndoPersistenceLevel(relpersist);

	if (!GetCurrentUndoRange(&begin, &end, p))
		PG_RETURN_NULL();

	res_str = psprintf(UndoRecPtrFormat, end);
	PG_RETURN_DATUM(CStringGetTextDatum(res_str));
}

/* TODO Introduce SQL type for UndoRecPtr */
static UndoRecPtr
urp_from_string(char *str)
{
	uint32		   high, low;
	UndoRecPtr	   result;

	if (sscanf(str, "%08X%08X", &high, &low) != 2)
		elog(ERROR, "could not recognize UNDO pointer value \"%s\"", str);

	result = (((UndoRecPtr) high) << 32) + low;

	return result;
}

typedef struct UndoScanState
{
	/* User input. */
	UndoRecPtr	start;
	UndoRecPtr	end;

	/* Start of the current page. */
	UndoRecPtr	cur_page;
	UndoLogParserState	parser;
	UndoSegFile	seg;			/* The current segment. */

	int		cur_item;

	/* Found the first non-discarded page? */
	bool	found_page;
} UndoScanState;

/*
 * Parse the next undo log page and return true, or return false if suitable
 * page could not be found. Stop at when state->end or log end is reached.
 *
 * Parsing runs in the memory context passed by the caller.
 */
static bool
parse_next_page(UndoScanState *state, MemoryContext context, bool records)
{
	Buffer	buffer = InvalidBuffer;
	MemoryContext oldcontext;

	/* Stop if the parser encountered end of data. */
	if (state->parser.gap)
		return false;

	/* Retrieve the next page. */
	while (state->cur_page == InvalidUndoRecPtr ||
		   (state->cur_page < state->end && buffer == InvalidBuffer))
	{
		bool	page_exists;
		UndoLogOffset	page_start_off, seg_start_off;
		UndoSegFile		*seg = &state->seg;
		UndoLogNumber	logno;

		/* Invalid cur_page indicates that this is the first call. */
		if (state->cur_page == InvalidUndoRecPtr)
		{
			state->cur_page = state->start - UndoRecPtrGetPageOffset(state->start);
			if (state->cur_page >= state->end)
				return false;
		}

		logno = UndoRecPtrGetLogNo(state->cur_page);
		page_start_off = UndoRecPtrGetOffset(state->cur_page);
		seg_start_off = page_start_off - page_start_off % UndoLogSegmentSize;

		/*
		 * Do not try to read pages beyond the insertion position - attempt to
		 * do so can result in error because the segment might not exist.
		 *
		 * Try to used cached values of 'insert' so that we don't have to
		 * access the slot each time.
		 *
		 * XXX Is it worth to check the discard pointer too? Not sure, the
		 * worst case is that we'll quickly receive InvalidBuffer if the page
		 * we need is already discarded.
		 */
		if (strlen(seg->path) > 0 &&
			logno == seg->logno && seg_start_off == seg->offset &&
			page_start_off < seg->insert)
			page_exists = true;
		else
		{
			UndoLogSlot *slot;

			/*
			 * There's usually no relationship between two consecutive
			 * logs. If the transaction spans multiple logs, caller needs to
			 * handle them separately.
			 */
			if (strlen(seg->path) > 0 && logno != seg->logno)
				return false;

			/*
			 * We've either crossed segment/log boundary or suspect that the
			 * cached insert pointer is outdated (or not initialized yet). The
			 * slot needs to be checked.
			 */
			slot = UndoLogGetSlot(logno, true);
			if (slot != NULL)
			{
				LWLockAcquire(&slot->meta_lock, LW_SHARED);
				if (page_start_off < slot->meta.insert)
					page_exists = true;
				else
				{
					/*
					 * If there's no such page yet, it makes no sense to check
					 * the following ones.
					 */
					LWLockRelease(&slot->meta_lock);
					return false;
				}

				/* Update segment information if needed. */
				if (strlen(seg->path) == 0 ||
					slot->meta.logno != seg->logno ||
					seg_start_off != seg->offset)
				{
					seg->logno = slot->meta.logno;
					seg->offset = seg_start_off;
					seg->insert = slot->meta.insert;
					seg->persistence = slot->meta.persistence;
					UndoLogSegmentPath(seg->logno,
									   seg->offset / UndoLogSegmentSize,
									   slot->meta.tablespace,
									   seg->path);
				}
				else
					seg->insert = slot->meta.insert;
				LWLockRelease(&slot->meta_lock);
			}
			else
			{
				/* The whole log has been discarded. */
				page_exists = false;
			}
		}

		/*
		 * Try to read the page. We can still receive InvalidBuffer if the
		 * page was discarded concurrently.
		 */
		if (page_exists)
		{
			RelFileNode rnode;
			BlockNumber	blockno;

			UndoRecPtrAssignRelFileNode(rnode, state->cur_page);
			blockno = UndoRecPtrGetOffset(state->cur_page) / BLCKSZ;
			buffer = ReadBufferWithoutRelcache(SMGR_UNDO,
											   rnode,
											   MAIN_FORKNUM,
											   blockno,
											   RBM_NORMAL,
											   NULL,
											   seg->persistence);
		}

		/*
		 * We can only skip pages if we haven't started to process them,
		 * however gaps are not allowed in the middle of the processing.
		 */
		if (buffer == InvalidBuffer && state->found_page)
			return false;

		/* Found the page? */
		if (buffer != InvalidBuffer)
			break;
	}

	/* No more data found? */
	if (buffer == InvalidBuffer)
		return false;

	/* Make sure discarded pages are not silently skipped anymore. */
	state->found_page = true;

	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	/*
	 * Parse the page. Do so in the multi-call context so that data allocated
	 * by the parser do survive.
	 */
	oldcontext = MemoryContextSwitchTo(context);
	parse_undo_page(&state->parser, BufferGetPage(buffer),
					(state->cur_page % UndoLogSegmentSize) / BLCKSZ,
					&state->seg, records);
	state->cur_page += BLCKSZ;
	MemoryContextSwitchTo(oldcontext);
	UnlockReleaseBuffer(buffer);

	return true;
}

/*
 * Setup the multi-call context for get_undo_log_chunks().
 */
static void
setup_undo_log_items_context(PG_FUNCTION_ARGS,  FuncCallContext *funcctx,
							  UndoRecPtr start, UndoRecPtr end)
{
	MemoryContext oldcontext;
	UndoScanState	*state;
	TupleDesc	tupdesc;

	oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	state = (UndoScanState *) palloc(sizeof(UndoScanState));
	state->start = start;
	state->end = end;
	state->cur_page = InvalidUndoRecPtr;
	initialize_undo_parser(&state->parser);
	state->seg.path[0] = '\0';
	state->cur_item = 0;
	state->found_page = false;
	funcctx->user_fctx = (void *) state;

	funcctx->tuple_desc = BlessTupleDesc(tupdesc);
	MemoryContextSwitchTo(oldcontext);
}

static UndoPageItem *
get_next_undo_log_item(PG_FUNCTION_ARGS, FuncCallContext *funcctx,
					   bool record)
{
	UndoScanState	*state;
	UndoPageItem	*item;

	state = (UndoScanState *) funcctx->user_fctx;

	/* Need the next undo page? */
	if (state->cur_item >= state->parser.nitems)
	{
		bool	success;

		Assert(state->cur_item == state->parser.nitems);

		while ((success = parse_next_page(state,
										  funcctx->multi_call_memory_ctx,
										  record)))
		{
			/* Does any item start on the page? */
			if (state->parser.nitems > 0)
			{
				UndoPageItem	*first, *last;

				/*
				 * Although at least some part of the page should be in the
				 * requested range, it's not guaranteed that at least one item
				 * is in that range. Check if the range overlaps with the
				 * chunk array.
				 */
				first = &state->parser.items[0];
				last = &state->parser.items[state->parser.nitems - 1];
				if (last->location >= state->start &&
					first->location < state->end)
					/* Use this page. */
					break;
			}
		}

		/* No next page available? */
		if (!success)
		{
			finalize_undo_parser(&state->parser, record);
			return NULL;
		}

		state->cur_item = 0;
	}

	/* Skip chunks ahead of the requested range. */
	do
	{
		item = &state->parser.items[state->cur_item++];
	} while (item->location < state->start &&
			 state->cur_item < state->parser.nitems);
	/*
	 * At least the last item must be in the range, see the overlap check
	 * above.
	 */
	Assert(state->cur_item <= state->parser.nitems);

	/* Reached the end? */
	if (item->location >= state->end)
	{
		finalize_undo_parser(&state->parser, record);
		return NULL;
	}

	return item;
}

/*
 * XXX Currently we might display discarded chunks if the discard pointer was
 * in the middle of the containing page at the time we were checking it
 * (i.e. less then the whole page was discarded), or even if nothing of the
 * page was discarded at the time of our check but the discarding took place
 * before we could read and lock the buffer. Not sure these race conditions
 * are a problem, as the function is there primarily for debugging.
 */
static HeapTuple
get_next_undo_log_chunk(PG_FUNCTION_ARGS, FuncCallContext *funcctx)
{
	UndoPageItem	*item;
	UndoLogChunkInfo	*chunk;
	Datum	values[7];
	bool	isnull[7];

	item = get_next_undo_log_item(fcinfo, funcctx, false);
	if (item == NULL)
		return NULL;

	chunk = &item->u.chunk;

	/* Return the next tuple. */
	memset(isnull, false, sizeof(isnull));
	values[0] = CStringGetTextDatum(psprintf("%.6lX",
											 UndoRecPtrGetLogNo(item->location)));
	values[1] = CStringGetTextDatum(psprintf(UndoRecPtrFormat, item->location));
	if (chunk->hdr.previous_chunk != InvalidUndoRecPtr)
		values[2] = CStringGetTextDatum(psprintf(UndoRecPtrFormat,
												 chunk->hdr.previous_chunk));
	else
		isnull[2] = true;
	values[3] = UInt64GetDatum(chunk->hdr.size);
	values[4] = BoolGetDatum(chunk->hdr.discarded);
	if (chunk->hdr.type == URST_TRANSACTION)
	{
		XactUndoRecordSetHeader *hdr;
		TransactionId xid;

		hdr = &chunk->type_header.xact;
		xid = XidFromFullTransactionId(hdr->fxid);
		values[5] = CStringGetTextDatum("xact");

		/* Print the type header (only contained in the first chunk). */
		if (chunk->hdr.previous_chunk == InvalidUndoRecPtr)
		{
			/*
			 * Print out t/f rather than true/false so it's consistent with
			 * the 'discarded' bool value.
			 */
			values[6] = CStringGetTextDatum(psprintf("(xid=%u, dboid=%u, applied=%s)",
													 xid, hdr->dboid, hdr->applied ?
													 "t" : "f"));
		}
		else
			isnull[6] = true;
	}
	else
	{
		isnull[5] = true;
		isnull[6] = true;
	}

	return heap_form_tuple(funcctx->tuple_desc, values, isnull);
}

/*
 * Return set of undo log chunks between 'start' and 'end' location.
 *
 * TODO Handle the cases where the transaction crosses log boundary.
 */
Datum
pg_get_undo_log_chunks(PG_FUNCTION_ARGS)
{
	FuncCallContext	*funcctx;
	HeapTuple	tuple;

	if (SRF_IS_FIRSTCALL())
	{
		UndoRecPtr	start, end;

		start = urp_from_string(text_to_cstring(PG_GETARG_TEXT_PP(0)));
		end = urp_from_string(text_to_cstring(PG_GETARG_TEXT_PP(1)));
		funcctx = SRF_FIRSTCALL_INIT();

		setup_undo_log_items_context(fcinfo, funcctx, start, end);
	}
	funcctx = SRF_PERCALL_SETUP();

	tuple = get_next_undo_log_chunk(fcinfo, funcctx);

	if (tuple)
	{
		Datum result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	else
		SRF_RETURN_DONE(funcctx);
}

static HeapTuple
get_next_undo_log_record(PG_FUNCTION_ARGS, FuncCallContext *funcctx)
{
	UndoPageItem	*item;
	UndoNode	*node;
	WrittenUndoNode	wnode;
	const RmgrData *desc;
	StringInfoData	buf;
	Datum	values[4];
	bool	isnull[4];

	item = get_next_undo_log_item(fcinfo, funcctx, true);
	if (item == NULL)
		return NULL;

	node = &item->u.record;
	wnode.location = item->location;
	wnode.n = *node;

	/* Return the next tuple. */
	memset(isnull, false, sizeof(isnull));
	values[0] = CStringGetTextDatum(psprintf(UndoRecPtrFormat, item->location));
	values[1] = UInt64GetDatum(node->length);

	desc = &RmgrTable[node->rmid];
	values[2] = CStringGetTextDatum(desc->rm_name);

	initStringInfo(&buf);
	resetStringInfo(&buf);
	desc->rm_undo_desc(&buf, &wnode);
	values[3] = CStringGetTextDatum(buf.data);

	return heap_form_tuple(funcctx->tuple_desc, values, isnull);
}

Datum
pg_get_undo_log_records(PG_FUNCTION_ARGS)
{
	FuncCallContext	*funcctx;
	HeapTuple	tuple;

	if (SRF_IS_FIRSTCALL())
	{
		UndoRecPtr	start, end;

		start = urp_from_string(text_to_cstring(PG_GETARG_TEXT_PP(0)));
		end = urp_from_string(text_to_cstring(PG_GETARG_TEXT_PP(1)));
		funcctx = SRF_FIRSTCALL_INIT();

		setup_undo_log_items_context(fcinfo, funcctx, start, end);
	}
	funcctx = SRF_PERCALL_SETUP();

	tuple = get_next_undo_log_record(fcinfo, funcctx);

	if (tuple)
	{
		Datum result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	else
		SRF_RETURN_DONE(funcctx);
}
