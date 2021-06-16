/*-------------------------------------------------------------------------
 *
 * discardworker.h
 *	  interfaces for the undo discard worker
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/discardworker.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef DISCARDWORKER_H
#define DISCARDWORKER_H

#include "postgres.h"

extern void DiscardWorkerRegister(void);
extern void DiscardWorkerMain(Datum main_arg);

#endif
