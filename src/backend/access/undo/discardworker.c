/*-------------------------------------------------------------------------
 *
 * discardworker.c
 *	  automatic discarding of undo record sets
 *
 * This worker checks periodically whether there's enough new undo log written
 * and if so, it tries to discard as much as possible.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/undo/discardworker.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/discardworker.h"
#include "access/undolog.h"
#include "access/undorecordset.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/latch.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

void
DiscardWorkerRegister(void)
{
	BackgroundWorker bgw;

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "DiscardWorkerMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN,
			 "undo discard worker");
	snprintf(bgw.bgw_type, BGW_MAXLEN,
			 "undo discard worker");
	bgw.bgw_restart_time = 1;
	bgw.bgw_notify_pid = 0;
	bgw.bgw_main_arg = (Datum) 0;

	RegisterBackgroundWorker(&bgw);
}

void
DiscardWorkerMain(Datum main_arg)
{
	TimestampTz last_check_time = 0;
	bool	first = true;

	/*
	 * TODO Set handler for SIGHUP (SignalHandlerForConfigReload) if the
	 * worker eventually needs at least one GUC.
	 */

	BackgroundWorkerUnblockSignals();

	SetProcessingMode(NormalProcessing);

	/* We're going to deal with shared buffers. */
	Assert(CurrentResourceOwner == NULL);
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "discard worker");

	for (;;)
	{
		TimestampTz	next_check_time, now;
		long	wait_time;

		CHECK_FOR_INTERRUPTS();

		/* 10 seconds. */
		/* TODO Tune, or make it a GUC? */
#define	DISCARD_WORKER_NAPTIME	10000

		now = GetCurrentTimestamp();

		if (TimestampDifferenceExceeds(last_check_time, now,
									   DISCARD_WORKER_NAPTIME))
		{
			bool	do_process = false;

			/*
			 * Save the time before the processing starts so that there is a
			 * constant delay between the checks, i.e. the processing time
			 * does not make the next check shifted.
			 */
			last_check_time = now;

			/*
			 * Unless this is the first loop, check if there's enough work to
			 * do.
			 */
			if (!first)
			{
				UndoLogSlot	*slot = NULL;

				while ((slot = UndoLogGetNextSlot(slot)))
				{
					UndoLogOffset discard, insert;

					LWLockAcquire(&slot->meta_lock, LW_SHARED);
					discard = slot->meta.discard;
					insert = slot->meta.insert;
					LWLockRelease(&slot->meta_lock);

					Assert(insert >= discard);

					/*
					 * Currently we consider any amount of non-discarded log
					 * worth processing.
					 *
					 * TODO Make the check more sophisticated. There should
					 * probably be some threshold amount of non-discarded log
					 * which does not trigger processing. Also, if discarding
					 * is needed, we might want to consider the amount of log
					 * processed as well as the processing time, and adjust
					 * the naptime for the next iteration.
					 */
					if (insert > discard)
					{
						do_process = true;
						break;
					}
				}
			}
			else
				first = false;

			if (do_process)
			{
				elog(DEBUG1, "trying to discard undo log");
				AdvanceOldestXidHavingUndo();
				DiscardUndoRecordSetChunks();

				/* The processing took some time. */
				now = GetCurrentTimestamp();

				/* Compute the time remaining to the next check. */
				next_check_time = last_check_time +
					DISCARD_WORKER_NAPTIME * INT64CONST(1000);
				wait_time = TimestampDifferenceMilliseconds(now,
															next_check_time);
			}
			else
			{
				elog(DEBUG1, "no undo log to discard");

				/*
				 * No processing took place so the current time corresponds to
				 * the check we just performed.
				 */
				wait_time = DISCARD_WORKER_NAPTIME;
			}
			/*
			 * XXX Is it o.k. to neglect the check time, or should we distract
			 * it from wait_time (unless wait_time is already zero)?
			 */
		}
		else
		{
			/* Woke up too early from the last waiting, just wait again. */
			next_check_time = last_check_time +
				DISCARD_WORKER_NAPTIME * INT64CONST(1000);
			wait_time = TimestampDifferenceMilliseconds(now, next_check_time);
		}

		if (wait_time > 0)
		{
			int	rc;

			rc = WaitLatch(MyLatch,
						   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						   wait_time,
						   WAIT_EVENT_DISCARD_WORKER_MAIN);

			if (rc & WL_LATCH_SET)
				ResetLatch(MyLatch);
		}
	}
}
