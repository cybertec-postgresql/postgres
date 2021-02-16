/*
 * rmgrdesc.h
 *
 * pg_undo_dump resource managers declaration
 *
 * src/bin/pg_undo_dump/rmgrdesc.h
 */
#ifndef RMGRDESC_H
#define RMGRDESC_H

#include "access/xlogreader.h"
#include "lib/stringinfo.h"

typedef struct RmgrDescData
{
	const char *rm_name;
	void		(*undo_desc) (StringInfo buf, const WrittenUndoNode *record);
} RmgrDescData;

extern const RmgrDescData RmgrDescTable[];

#endif							/* RMGRDESC_H */
