/*-------------------------------------------------------------------------
 *
 * undoworker.c
 *	  Implementation of the undo apply worker.
 *
 * Currently, the worker is only used to apply the undo records that could not
 * be applied due to server crash. Using a (per-database) background worker
 * seems to be easier than teaching the startup process to use transactions
 * and to connect to all the existing databases.
 *
 * The original zheap implementation used the undo worker to apply the undo
 * records on behalf of regular backends, but that adds quite a bit of
 * complexity to the system. This functionality may be added in the future.
 *
 * TODO Coordinate (using barrier.c ?) the workers so that the chunk
 * information is only gathered once.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/undo/undoworker.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/relscan.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/undorecordset.h"
#include "access/undoworker.h"
#include "access/xact.h"
#include "catalog/pg_database.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "utils/relcache.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"

static void RegisterUndoWorker(Oid dboid);
static List *get_database_list(void);

void
UndoLauncherRegister(void)
{
	BackgroundWorker bgw;

	memset(&bgw, 0, sizeof(BackgroundWorker));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "UndoLauncherMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "undo launcher");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "undo launcher");
	bgw.bgw_restart_time = BGW_NEVER_RESTART;

	RegisterBackgroundWorker(&bgw);
}

/*
 * Retrieve database OIDs and launch the undo worker for each.
 */
void
UndoLauncherMain(Datum main_arg)
{
	List	   *dboids;
	ListCell   *lc;

	/* Announce that we are running. */
	elog(DEBUG2, "undo launcher started");

	/* No special signal handlers needed. */
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection(NULL, NULL, 0);

	dboids = get_database_list();
	foreach(lc, dboids)
	{
		Oid			dboid = lfirst_oid(lc);

		RegisterUndoWorker(dboid);
	}

	elog(DEBUG2, "undo launcher exiting");
	proc_exit(0);
}

/*
 * Register the undo launcher for given database.
 */
static void
RegisterUndoWorker(Oid dboid)
{
	BackgroundWorker bgw;

	memset(&bgw, 0, sizeof(BackgroundWorker));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "UndoWorkerMain");
	/* XXX Print out database name rather than oid? */
	snprintf(bgw.bgw_name, BGW_MAXLEN, "undo worker for database %u", dboid);
	snprintf(bgw.bgw_type, BGW_MAXLEN, "undo worker");
	bgw.bgw_restart_time = BGW_NEVER_RESTART;
	bgw.bgw_main_arg = ObjectIdGetDatum(dboid);
	bgw.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&bgw, NULL))
		ereport(WARNING,
				(errmsg("could not register undo worker for database %u",
						dboid)));
}

/*
 * Entry point and main loop for undo worker processes.
 */
void
UndoWorkerMain(Datum main_arg)
{
	Oid			dboid = DatumGetObjectId(main_arg);

	/* Announce that we are running. */
	elog(DEBUG2, "undo worker for database %u started", dboid);

	/* No special signal handlers needed. */
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnectionByOid(dboid, InvalidOid, 0);

	/* Do the actual work. */
	ApplyPendingUndo();

	elog(DEBUG2, "undo worker for database %u exiting", dboid);
	proc_exit(0);
}


/*
 * Retrieve the list of databases for which the undo worker should be started.
 *
 * This is simlar to the get_database_list function in autovaccum.c, but
 * probably not enough to reuse the code.
 */
static List *
get_database_list(void)
{
	List	   *dblist = NIL;
	Relation	rel;
	TableScanDesc scan;
	HeapTuple	tup;
	MemoryContext resultcxt;

	/* This is the context that we will allocate our output data in */
	resultcxt = CurrentMemoryContext;

	StartTransactionCommand();
	(void) GetTransactionSnapshot();

	rel = table_open(DatabaseRelationId, AccessShareLock);
	scan = table_beginscan_catalog(rel, 0, NULL);

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_database pgdatabase = (Form_pg_database) GETSTRUCT(tup);
		MemoryContext oldcxt;

		if (strcmp(NameStr(pgdatabase->datname), "template0") == 0)
			continue;

		oldcxt = MemoryContextSwitchTo(resultcxt);

		dblist = lappend_oid(dblist, pgdatabase->oid);
		MemoryContextSwitchTo(oldcxt);
	}

	table_endscan(scan);
	table_close(rel, AccessShareLock);

	CommitTransactionCommand();

	return dblist;
}
