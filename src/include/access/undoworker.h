/*-------------------------------------------------------------------------
 *
 * undoworker.h
 *	  interfaces for the undo apply worker
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/undoworker.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef UNDOWORKER_H
#define UNDOWORKER_H

#include "postgres.h"

extern void UndoLauncherRegister(void);
extern void UndoLauncherMain(Datum main_arg);
extern void UndoWorkerMain(Datum main_arg);

#endif
