/*-------------------------------------------------------------------------
 *
 * zheapam_xlog.h
 *	  POSTGRES zheap access XLOG definitions.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/zheapam_xlog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ZHEAP_XLOG_H
#define ZHEAP_XLOG_H

#include "postgres.h"

#include "access/hio.h"
#include "access/xlogreader.h"
#include "access/zhtup.h"
#include "storage/freespace.h"
#include "utils/rel.h"
#include "utils/snapshot.h"

/*
 * WAL record definitions for zheapam.c's WAL operations
 *
 * XLOG allows to store some information in high 4 bits of log
 * record xl_info field.  We use 3 for opcode and one for init bit.
 */
#define XLOG_ZHEAP_INSERT				0x00
#define XLOG_ZHEAP_DELETE				0x10
#define XLOG_ZHEAP_UPDATE				0x20
#define XLOG_ZHEAP_MULTI_INSERT			0x30
#define XLOG_ZHEAP_FREEZE_XACT_SLOT		0x40
#define XLOG_ZHEAP_INVALID_XACT_SLOT	0x50
#define XLOG_ZHEAP_LOCK					0x60
#define XLOG_ZHEAP_CLEAN				0x70

#define	XLOG_ZHEAP_OPMASK				0x70

/*
 * When we insert 1st item on new page in INSERT, NON-INPLACE-UPDATE,
 * or MULTI_INSERT, we can (and we do) restore entire page in redo
 */
#define XLOG_ZHEAP_INIT_PAGE		0x80

/*
 * We ran out of opcodes, so zheapam.c now has a second RmgrId.  These opcodes
 * are associated with RM_ZHEAP2_ID, but are not logically different from
 * the ones above associated with RM_ZHEAP_ID.  XLOG_ZHEAP_OPMASK applies to
 * these, too.
 */
#define XLOG_ZHEAP_CONFIRM		0x00
#define XLOG_ZHEAP_UNUSED		0x10
#define XLOG_ZHEAP_VISIBLE		0x20

/*
 * All that we need to regenerate the meta-data page
 */
typedef struct xl_zheap_metadata
{
	uint32		first_used_tpd_page;
	uint32		last_used_tpd_page;
} xl_zheap_metadata;

#define SizeOfMetaData	(offsetof(xl_zheap_metadata, last_used_tpd_page) + sizeof(uint32))

/* common undo record related info */
typedef struct xl_undo_header
{
	Oid			reloid;			/* relation OID */
	UndoRecPtr	blkprev;		/* the previous undo for block */
} xl_undo_header;

#define SizeOfUndoHeader	(offsetof(xl_undo_header, blkprev) + sizeof(uint64))

/*
 * xl_zheap_insert/xl_zheap_multi_insert flag values, 8 bits are available.
 */
#define XLZ_INSERT_ALL_VISIBLE_CLEARED			(1<<0)
#define XLZ_INSERT_LAST_IN_MULTI				(1<<1)
#define XLZ_INSERT_IS_SPECULATIVE				(1<<2)
#define XLZ_INSERT_CONTAINS_NEW_TUPLE			(1<<3)
#define XLZ_INSERT_CONTAINS_TPD_SLOT			(1<<4)
#define XLZ_INSERT_IS_FROZEN					(1<<5)
#define XLZ_INSERT_ON_TOAST_RELATION			(1<<6)
#define XLZ_INSERT_CONTAINS_SUBXID				(1<<7)

/*
 * NOTE: t_hoff could be recomputed, but we may as well store it because
 * it will come for free due to alignment considerations.
 */
typedef struct xl_zheap_header
{
	uint16		t_infomask2;
	uint16		t_infomask;
	uint8		t_hoff;
} xl_zheap_header;

#define SizeOfZHeapHeader	(offsetof(xl_zheap_header, t_hoff) + sizeof(uint8))

/* This is what we need to know about insert */
typedef struct xl_zheap_insert
{
	/* heap record related info */
	OffsetNumber offnum;		/* inserted tuple's offset */
	uint8		flags;

	/*
	 * subxid follows the structure iff XLZ_INSERT_CONTAINS_SUBXID is set.
	 *
	 * This is for logical decoding to be aware of subtransaction assignment
	 * to the top-level transaction. Unfortunately we cannot use the
	 * XLOG_INCLUDE_XID flag because zheap stores the top-level XID anyway. If
	 * we just reverted the meaning of that flag for zheap and used it to
	 * store the subtransaction XID, it wouldn't suffice because the flag only
	 * ensures that the assignment is recorded once per subtransaction.
	 * However each record (which was issued in a subtransaction) needs the
	 * subtransaction XID for logical decoding to work.
	 */

	/* xl_zheap_header & TUPLE DATA in backup block 0 */
} xl_zheap_insert;

#define SizeOfZHeapInsert	(offsetof(xl_zheap_insert, flags) + sizeof(uint8))

/*
 * xl_zheap_delete flag values, 8 bits are available.
 */
/* PD_ALL_VISIBLE was cleared */
#define XLZ_DELETE_ALL_VISIBLE_CLEARED			(1<<0)
/* undo tuple is present in xlog record? */
#define XLZ_HAS_DELETE_UNDOTUPLE				(1<<1)
#define XLZ_DELETE_CONTAINS_TPD_SLOT			(1<<2)
#define XLZ_DELETE_CONTAINS_SUBXACT				(1<<3)
#define XLZ_DELETE_IS_PARTITION_MOVE			(1<<4)
#define XLZ_DELETE_CONTAINS_SUBXID				(1<<5)
#define XLZ_DELETE_CONTAINS_OLD_KEYS_EXT		(1<<6)

/* This is what we need to know about delete */
typedef struct xl_zheap_delete
{
	/* info related to undo record */
	TransactionId prevxid;		/* transaction id that has modified the tuple
								 * written in undo record for delete operation */

	/* zheap related info */
	OffsetNumber offnum;		/* deleted tuple's offset */
	uint16		infomask;		/* lock mode */
	uint16		trans_slot_id;	/* transaction slot id */
	uint8		flags;

	/*
	 * subxid follows the structure iff XLZ_DELETE_CONTAINS_SUBXID is set. See
	 * xl_zheap_insert for more information.
	 */
} xl_zheap_delete;

#define SizeOfZHeapDelete	(offsetof(xl_zheap_delete, flags) + sizeof(uint8))

/*
 * xl_zheap_update flag values, 8 bits are available.
 */
/* PD_ALL_VISIBLE was cleared */
#define XLZ_UPDATE_OLD_ALL_VISIBLE_CLEARED		(1<<0)
/* PD_ALL_VISIBLE was cleared in the 2nd page */
#define XLZ_UPDATE_NEW_ALL_VISIBLE_CLEARED		(1<<1)
#define XLZ_UPDATE_PREFIX_FROM_OLD				(1<<2)
#define XLZ_UPDATE_SUFFIX_FROM_OLD				(1<<3)
#define	XLZ_NON_INPLACE_UPDATE					(1<<4)
#define	XLZ_HAS_UPDATE_UNDOTUPLE				(1<<5)
#define	XLZ_UPDATE_OLD_CONTAINS_TPD_SLOT		(1<<6)
#define	XLZ_UPDATE_NEW_CONTAINS_TPD_SLOT		(1<<7)
#define XLZ_UPDATE_CONTAINS_SUBXACT				(1<<8)
#define XLZ_UPDATE_CONTAINS_SUBXID				(1<<9)
#define XLZ_UPDATE_IDENTITY_CHANGED				(1<<10)
#define XLZ_UPDATE_CONTAINS_OLD_KEYS_EXT		(1<<11)

/*
 * This is what we need to know about update|inplace_update
 *
 * Backup blk 0: new page
 *
 * If XLOG_ZHEAP_PREFIX_FROM_OLD or XLOG_ZHEAP_SUFFIX_FROM_OLD flags are set,
 * the prefix and/or suffix come first, as one or two uint16s.
 *
 * After that, xl_zheap_header and new tuple data follow.  The new tuple
 * data doesn't include the prefix and suffix, which are copied from the
 * old tuple on replay.
 *
 * Backup blk 1: old page, if different. (no data, just a reference to the blk)
 */
typedef struct xl_zheap_update
{
	/* info related to undo record */
	TransactionId prevxid;		/* transaction id that has modified the tuple
								 * written in undo record for delete operation */
	/* zheap related info */
	OffsetNumber old_offnum;	/* old tuple's offset */
	uint16		old_infomask;	/* infomask bits to set on old tuple */
	uint16		old_trans_slot_id;	/* old tuple's transaction slot id */
	uint16		flags;
	OffsetNumber new_offnum;	/* new tuple's offset */

	/*
	 * subxid follows the structure iff XLZ_UPDATE_CONTAINS_SUBXID is set. See
	 * xl_zheap_insert for more information.
	 */
} xl_zheap_update;

#define SizeOfZHeapUpdate	(offsetof(xl_zheap_update, new_offnum) + sizeof(OffsetNumber))

/* This is what we need to know for freezing transaction slots */
typedef struct xl_zheap_freeze_xact_slot
{
	TransactionId lastestFrozenXid; /* latest frozen xid */
	uint16		nFrozen;		/* number of transaction slots to freeze */
} xl_zheap_freeze_xact_slot;

#define SizeOfZHeapFreezeXactSlot	(offsetof(xl_zheap_freeze_xact_slot, nFrozen) + sizeof(uint16))

/*
 * xl_zheap_lock flag values, 8 bits are available.
 */
#define XLZ_LOCK_TRANS_SLOT_FOR_UREC		(1<<0)
#define XLZ_LOCK_CONTAINS_TPD_SLOT			(1<<1)
#define XLZ_LOCK_CONTAINS_SUBXACT			(1<<2)
#define XLZ_LOCK_FOR_UPDATE					(1<<3)

/* This is what we need to know about zheap lock tuple. */
typedef struct xl_zheap_lock
{
	/* info related to undo record */
	TransactionId prev_xid;
	/* zheap related info */
	OffsetNumber offnum;		/* locked tuple's offset */
	uint16		infomask;		/* lock mode */
	uint16		trans_slot_id;	/* transaction slot id */
	uint8		flags;
} xl_zheap_lock;

#define SizeOfZHeapLock    (offsetof(xl_zheap_lock, flags) + sizeof(uint8))

/*
 * This is what we need to know about a multi-insert.
 *
 * The main data of the record consists of this xl_zheap_multi_insert header,
 * 'offset ranges', tpd transaction slot number and optionally the
 * subtransaction XID.
 *
 * In block 0's data portion, there is an xl_multi_insert_ztuple struct,
 * followed by the tuple data for each tuple. There is padding to align
 * each xl_zheap_multi_insert struct.
 */
typedef struct xl_zheap_multi_insert
{
	/* zheap record related info */
	uint8		flags;
	uint16		ntuples;

	/*
	 * subxid follows the structure iff XLZ_INSERT_CONTAINS_SUBXID is set. See
	 * xl_zheap_insert for more information.
	 */
} xl_zheap_multi_insert;

#define SizeOfZHeapMultiInsert	(offsetof(xl_zheap_multi_insert, ntuples) + sizeof(uint16))

typedef struct xl_multi_insert_ztuple
{
	uint16		datalen;		/* size of tuple data that follows */
	uint16		t_infomask2;
	uint16		t_infomask;
	uint8		t_hoff;
	/* TUPLE DATA FOLLOWS AT END OF STRUCT */
} xl_multi_insert_ztuple;

#define SizeOfMultiInsertZTuple	(offsetof(xl_multi_insert_ztuple, t_hoff) + sizeof(uint8))

/*
 * This is what we need to know about vacuum page cleanup/redirect
 *
 * The array of OffsetNumbers following the fixed part of the record contains:
 * for each redirected item: the item offset, then the offset redirected to
 * for each now-dead item: the item offset for each now-unused item: the item offset
 * The total number of OffsetNumbers is therefore 2*nredirected+ndead+nunused.
 * Note that nunused is not explicitly stored, but may be found by reference to the
 * total record length.
 */
#define XLZ_CLEAN_CONTAINS_OFFSET			(1<<0)
#define XLZ_CLEAN_ALLOW_PRUNING				(1<<1)

typedef struct xl_zheap_clean
{

	TransactionId latestRemovedXid;
	uint16		ndeleted;
	uint16		ndead;
	uint8		flags;
	/* OFFSET NUMBERS are in the block reference 0 */
} xl_zheap_clean;

#define SizeOfZHeapClean (offsetof(xl_zheap_clean, flags) + sizeof(uint8))

#define XLZ_UNUSED_ALLOW_PRUNING				(1<<0)

typedef struct xl_zheap_unused
{

	TransactionId latestRemovedXid;
	uint16		nunused;
	uint16		trans_slot_id;
	uint8		flags;
	/* OFFSET NUMBERS are in the block reference 0 */
} xl_zheap_unused;

#define SizeOfZHeapUnused (offsetof(xl_zheap_unused, flags) + sizeof(uint8))

/* This is what we need to know about confirmation of speculative insertion */
/*
 * xl_zheap_confirm flag values, 8 bits are available.
 */
/* speculative insertion is successful */
#define XLZ_SPEC_INSERT_SUCCESS			(1<<0)
/* speculative insertion failed */
#define XLZ_SPEC_INSERT_FAILED				(1<<1)
typedef struct xl_zheap_confirm
{
	OffsetNumber offnum;		/* confirmed tuple's offset on page */
	uint8		flags;
	uint16		trans_slot_id;
} xl_zheap_confirm;

#define SizeOfZHeapConfirm	(offsetof(xl_zheap_confirm, trans_slot_id) + sizeof(uint16))

/*
 * This is what we need to know about setting a visibility map bit
 */
typedef struct xl_zheap_visible
{
	TransactionId cutoff_xid;
	BlockNumber heapBlk;
	uint8		flags;
} xl_zheap_visible;

#define SizeOfZHeapVisible (offsetof(xl_zheap_visible, flags) + sizeof(uint8))

/*
 * WAL record definitions for zundo.c's WAL operations
 */
#define XLOG_ZUNDO_PAGE				0x00
#define XLOG_ZUNDO_RESET_SLOT		0x10

/*
 * xl_undoaction_page flag values, 8 bits are available.
 */
#define XLU_PAGE_CONTAINS_TPD_SLOT			(1<<0)
#define XLU_PAGE_CLEAR_VISIBILITY_MAP		(1<<1)
#define XLU_CONTAINS_TPD_OFFSET_MAP			(1<<2)
#define XLU_INIT_PAGE						(1<<3)

/* This is what we need to know about delete */
typedef struct xl_zundo_page
{
	UndoRecPtr	urec_ptr;
	FullTransactionId fxid;
	int			trans_slot_id;	/* transaction slot id */
} xl_zundo_page;

#define SizeOfZUndoPage	(offsetof(xl_zundo_page, trans_slot_id) + sizeof(int))

/*
 * xl_undoaction_reset_slot flag values, 8 bits are available.
 */
#define XLU_RESET_CONTAINS_TPD_SLOT			(1<<0)

/* This is what we need to know about delete */
typedef struct xl_zundo_reset_slot
{
	UndoRecPtr	urec_ptr;
	int			trans_slot_id;	/* transaction slot id */
	uint8		flags;
} xl_zundo_reset_slot;

#define SizeOfZUndoResetSlot	(offsetof(xl_zundo_reset_slot, flags) + sizeof(uint8))

extern void zheap_redo(XLogReaderState *record);
extern void zheap_desc(StringInfo buf, XLogReaderState *record);
extern const char *zheap_identify(uint8 info);
extern void zheap2_redo(XLogReaderState *record);
extern void zheap_mask(char *pagedata, BlockNumber blkno);
extern void zheap2_desc(StringInfo buf, XLogReaderState *record);
extern const char *zheap2_identify(uint8 info);
extern void zundo_redo(XLogReaderState *record);
extern void zundo_desc(StringInfo buf, XLogReaderState *record);
extern const char *zundo_identify(uint8 info);

#endif							/* ZHEAP_XLOG_H */
