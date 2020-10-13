/*
 *	server.c
 *
 *	database server functions
 *
 *	Portions Copyright (c) 2019, Cybertec Schönig & Schönig GmbH
 *	Copyright (c) 2010-2019, PostgreSQL Global Development Group
 *	src/bin/pg_upgrade/server.c
 */

#include "postgres_fe.h"

#include "fe_utils/connect.h"
#include "fe_utils/string_utils.h"
#include "pg_upgrade.h"


static PGconn *get_db_conn(ClusterInfo *cluster, const char *db_name);


/*
 * connectToServer()
 *
 *	Connects to the desired database on the designated server.
 *	If the connection attempt fails, this function logs an error
 *	message and calls exit() to kill the program.
 */
PGconn *
connectToServer(ClusterInfo *cluster, const char *db_name)
{
	PGconn	   *conn = get_db_conn(cluster, db_name);

	if (conn == NULL || PQstatus(conn) != CONNECTION_OK)
	{
		pg_log(PG_REPORT, "connection to database failed: %s",
			   PQerrorMessage(conn));

		if (conn)
			PQfinish(conn);

		printf(_("Failure, exiting\n"));
		exit(1);
	}

	PQclear(executeQueryOrDie(conn, ALWAYS_SECURE_SEARCH_PATH_SQL));

	return conn;
}


/*
 * get_db_conn()
 *
 * get database connection, using named database + standard params for cluster
 */
static PGconn *
get_db_conn(ClusterInfo *cluster, const char *db_name)
{
	PQExpBufferData conn_opts;
	PGconn	   *conn;

	/* Build connection string with proper quoting */
	initPQExpBuffer(&conn_opts);
	appendPQExpBufferStr(&conn_opts, "dbname=");
	appendConnStrVal(&conn_opts, db_name);
	appendPQExpBufferStr(&conn_opts, " user=");
	appendConnStrVal(&conn_opts, os_info.user);
	appendPQExpBuffer(&conn_opts, " port=%d", cluster->port);
	if (cluster->sockdir)
	{
		appendPQExpBufferStr(&conn_opts, " host=");
		appendConnStrVal(&conn_opts, cluster->sockdir);
	}

	conn = PQconnectdb(conn_opts.data);
	termPQExpBuffer(&conn_opts);
	return conn;
}


/*
 * cluster_conn_opts()
 *
 * Return standard command-line options for connecting to this cluster when
 * using psql, pg_dump, etc.  Ideally this would match what get_db_conn()
 * sets, but the utilities we need aren't very consistent about the treatment
 * of database name options, so we leave that out.
 *
 * Result is valid until the next call to this function.
 */
char *
cluster_conn_opts(ClusterInfo *cluster)
{
	static PQExpBuffer buf;

	if (buf == NULL)
		buf = createPQExpBuffer();
	else
		resetPQExpBuffer(buf);

	if (cluster->sockdir)
	{
		appendPQExpBufferStr(buf, "--host ");
		appendShellString(buf, cluster->sockdir);
		appendPQExpBufferChar(buf, ' ');
	}
	appendPQExpBuffer(buf, "--port %d --username ", cluster->port);
	appendShellString(buf, os_info.user);

	return buf->data;
}


/*
 * executeQueryOrDie()
 *
 *	Formats a query string from the given arguments and executes the
 *	resulting query.  If the query fails, this function logs an error
 *	message and calls exit() to kill the program.
 */
PGresult *
executeQueryOrDie(PGconn *conn, const char *fmt,...)
{
	static char query[QUERY_ALLOC];
	va_list		args;
	PGresult   *result;
	ExecStatusType status;

	va_start(args, fmt);
	vsnprintf(query, sizeof(query), fmt, args);
	va_end(args);

	pg_log(PG_VERBOSE, "executing: %s\n", query);
	result = PQexec(conn, query);
	status = PQresultStatus(result);

	if ((status != PGRES_TUPLES_OK) && (status != PGRES_COMMAND_OK))
	{
		pg_log(PG_REPORT, "SQL command failed\n%s\n%s", query,
			   PQerrorMessage(conn));
		PQclear(result);
		PQfinish(conn);
		printf(_("Failure, exiting\n"));
		exit(1);
	}
	else
		return result;
}


/*
 * get_major_server_version()
 *
 * gets the version (in unsigned int form) for the given datadir. Assumes
 * that datadir is an absolute path to a valid pgdata directory. The version
 * is retrieved by reading the PG_VERSION file.
 */
uint32
get_major_server_version(ClusterInfo *cluster)
{
	FILE	   *version_fd;
	char		ver_filename[MAXPGPATH];
	int			v1 = 0,
				v2 = 0;

	snprintf(ver_filename, sizeof(ver_filename), "%s/PG_VERSION",
			 cluster->pgdata);
	if ((version_fd = fopen(ver_filename, "r")) == NULL)
		pg_fatal("could not open version file \"%s\": %m\n", ver_filename);

	if (fscanf(version_fd, "%63s", cluster->major_version_str) == 0 ||
		sscanf(cluster->major_version_str, "%d.%d", &v1, &v2) < 1)
		pg_fatal("could not parse version file \"%s\"\n", ver_filename);

	fclose(version_fd);

	if (v1 < 10)
	{
		/* old style, e.g. 9.6.1 */
		return v1 * 10000 + v2 * 100;
	}
	else
	{
		/* new style, e.g. 10.1 */
		return v1 * 10000;
	}
}


static void
stop_postmaster_atexit(void)
{
	stop_postmaster(true);
}


bool
start_postmaster(ClusterInfo *cluster, bool report_and_exit_on_error)
{
	char		cmd[MAXPGPATH * 4 + 1000];
	PGconn	   *conn;
	bool		pg_ctl_return = false;
	char		socket_string[MAXPGPATH + 200];
	char		encryption_key_port_opt[64];

	static bool exit_hook_registered = false;

	encryption_key_port_opt[0] = '\0';

	if (!exit_hook_registered)
	{
		atexit(stop_postmaster_atexit);
		exit_hook_registered = true;
	}

	socket_string[0] = '\0';

#ifdef HAVE_UNIX_SOCKETS
	/* prevent TCP/IP connections, restrict socket access */
	strcat(socket_string,
		   " -c listen_addresses='' -c unix_socket_permissions=0700");

	/* Have a sockdir?	Tell the postmaster. */
	if (cluster->sockdir)
		snprintf(socket_string + strlen(socket_string),
				 sizeof(socket_string) - strlen(socket_string),
				 " -c %s='%s'",
				 (GET_MAJOR_VERSION(cluster->major_version) < 903) ?
				 "unix_socket_directory" : "unix_socket_directories",
				 cluster->sockdir);
#endif

	/*
	 * Since PG 9.1, we have used -b to disable autovacuum.  For earlier
	 * releases, setting autovacuum=off disables cleanup vacuum and analyze,
	 * but freeze vacuums can still happen, so we set
	 * autovacuum_freeze_max_age to its maximum.
	 * (autovacuum_multixact_freeze_max_age was introduced after 9.1, so there
	 * is no need to set that.)  We assume all datfrozenxid and relfrozenxid
	 * values are less than a gap of 2000000000 from the current xid counter,
	 * so autovacuum will not touch them.
	 *
	 * Turn off durability requirements to improve object creation speed, and
	 * we only modify the new cluster, so only use it there.  If there is a
	 * crash, the new cluster has to be recreated anyway.  fsync=off is a big
	 * win on ext4.
	 *
	 * Force vacuum_defer_cleanup_age to 0 on the new cluster, so that
	 * vacuumdb --freeze actually freezes the tuples.
	 */
#ifndef HAVE_UNIX_SOCKETS
	/* Make sure pg_ctl sends the encryption key to the correct port. */
	sprintf(encryption_key_port_opt, " --encryption-key-port \"%d\"",
			cluster->port);
#endif
	snprintf(cmd, sizeof(cmd),
			 "\"%s/pg_ctl\"%s%s -w -l \"%s\" -D \"%s\" -o \"-p %d%s%s %s%s\" start",
			 cluster->bindir,
			 encryption_key_command_opt,
			 encryption_key_port_opt,
			 SERVER_LOG_FILE, cluster->pgconfig, cluster->port,
			 (cluster->controldata.cat_ver >=
			  BINARY_UPGRADE_SERVER_FLAG_CAT_VER) ? " -b" :
			 " -c autovacuum=off -c autovacuum_freeze_max_age=2000000000",
			 (cluster == &new_cluster) ?
			 " -c synchronous_commit=off -c fsync=off -c full_page_writes=off -c vacuum_defer_cleanup_age=0" : "",
			 cluster->pgopts ? cluster->pgopts : "", socket_string);

	/*
	 * If encryption key needs to be sent, run a separate process now and let
	 * it send the key to the postmaster.  We cannot send the key later in the
	 * current process because the exec_prog call below blocks until the
	 * postmaster succeeds or fails to start (and it will definitely fail if
	 * it receives no key).
	 */
	if (encryption_setup_done && !cluster->has_encr_key_cmd)
#ifdef USE_ENCRYPTION
	{
		SendKeyArgs	sk_args;
		char	port_str[6];
#ifndef WIN32
		pid_t sender;
#else
		HANDLE sender;
#endif

		snprintf(port_str, sizeof(port_str), "%d", cluster->port);

		/* in child process */
		sk_args.host = cluster->sockdir; /* If NULL, then libpq will use
										  * its default. */
		sk_args.port = port_str;
		sk_args.encryption_key = encryption_key;
		/* XXX Find out the postmaster PID ? */
		sk_args.pm_pid = 0;
		sk_args.error_msg = NULL;

#ifndef WIN32
		pg_log(PG_VERBOSE, "sending encryption key to postmaster\n");
		sender = fork();
		if (sender == 0)
		{
			send_key_to_postmaster(&sk_args);
			if (sk_args.error_msg)
				pg_fatal("%s", sk_args.error_msg);
			exit(EXIT_SUCCESS);
		}
		else if (sender < 0)
			pg_fatal("could not create key sender process");
#else	/* WIN32 */
		pg_log(PG_VERBOSE, "sending encryption key to postmaster\n");
		sender = _beginthreadex(NULL, 0, (void *) send_key_to_postmaster, &sk_args, 0, NULL);
		if (sender == 0)
			pg_fatal("could not create background thread: %m");
#endif	/* WIN32 */
	}
#else	/* USE_ENCRYPTION */
	{
		/* User should not be able to enable encryption. */
		Assert(false);
	}
#endif	/* USE_ENCRYPTION */

	/*
	 * Don't throw an error right away, let connecting throw the error because
	 * it might supply a reason for the failure.
	 */
	pg_ctl_return = exec_prog(SERVER_START_LOG_FILE,
	/* pass both file names if they differ */
							  (strcmp(SERVER_LOG_FILE,
									  SERVER_START_LOG_FILE) != 0) ?
							  SERVER_LOG_FILE : NULL,
							  report_and_exit_on_error, false, NULL,
							  "%s", cmd);

	/* Did it fail and we are just testing if the server could be started? */
	if (!pg_ctl_return && !report_and_exit_on_error)
		return false;

	/*
	 * We set this here to make sure atexit() shuts down the server, but only
	 * if we started the server successfully.  We do it before checking for
	 * connectivity in case the server started but there is a connectivity
	 * failure.  If pg_ctl did not return success, we will exit below.
	 *
	 * Pre-9.1 servers do not have PQping(), so we could be leaving the server
	 * running if authentication was misconfigured, so someday we might went
	 * to be more aggressive about doing server shutdowns even if pg_ctl
	 * fails, but now (2013-08-14) it seems prudent to be cautious.  We don't
	 * want to shutdown a server that might have been accidentally started
	 * during the upgrade.
	 */
	if (pg_ctl_return)
		os_info.running_cluster = cluster;

	/*
	 * pg_ctl -w might have failed because the server couldn't be started, or
	 * there might have been a connection problem in _checking_ if the server
	 * has started.  Therefore, even if pg_ctl failed, we continue and test
	 * for connectivity in case we get a connection reason for the failure.
	 */
	if ((conn = get_db_conn(cluster, "template1")) == NULL ||
		PQstatus(conn) != CONNECTION_OK)
	{
		pg_log(PG_REPORT, "\nconnection to database failed: %s",
			   PQerrorMessage(conn));
		if (conn)
			PQfinish(conn);
		if (cluster == &old_cluster)
			pg_fatal("could not connect to source postmaster started with the command:\n"
					 "%s\n",
					 cmd);
		else
			pg_fatal("could not connect to target postmaster started with the command:\n"
					 "%s\n",
					 cmd);
	}
	PQfinish(conn);

	/*
	 * If pg_ctl failed, and the connection didn't fail, and
	 * report_and_exit_on_error is enabled, fail now.  This could happen if
	 * the server was already running.
	 */
	if (!pg_ctl_return)
	{
		if (cluster == &old_cluster)
			pg_fatal("pg_ctl failed to start the source server, or connection failed\n");
		else
			pg_fatal("pg_ctl failed to start the target server, or connection failed\n");
	}

	return true;
}


void
stop_postmaster(bool in_atexit)
{
	ClusterInfo *cluster;

	if (os_info.running_cluster == &old_cluster)
		cluster = &old_cluster;
	else if (os_info.running_cluster == &new_cluster)
		cluster = &new_cluster;
	else
		return;					/* no cluster running */

	exec_prog(SERVER_STOP_LOG_FILE, NULL, !in_atexit, !in_atexit, NULL,
			  "\"%s/pg_ctl\" -w -D \"%s\" -o \"%s\" %s stop",
			  cluster->bindir, cluster->pgconfig,
			  cluster->pgopts ? cluster->pgopts : "",
			  in_atexit ? "-m fast" : "-m smart");

	os_info.running_cluster = NULL;
}


/*
 * check_pghost_envvar()
 *
 * Tests that PGHOST does not point to a non-local server
 */
void
check_pghost_envvar(void)
{
	PQconninfoOption *option;
	PQconninfoOption *start;

	/* Get valid libpq env vars from the PQconndefaults function */

	start = PQconndefaults();

	if (!start)
		pg_fatal("out of memory\n");

	for (option = start; option->keyword != NULL; option++)
	{
		if (option->envvar && (strcmp(option->envvar, "PGHOST") == 0 ||
							   strcmp(option->envvar, "PGHOSTADDR") == 0))
		{
			const char *value = getenv(option->envvar);

			if (value && strlen(value) > 0 &&
			/* check for 'local' host values */
				(strcmp(value, "localhost") != 0 && strcmp(value, "127.0.0.1") != 0 &&
				 strcmp(value, "::1") != 0 && value[0] != '/'))
				pg_fatal("libpq environment variable %s has a non-local server value: %s\n",
						 option->envvar, value);
		}
	}

	/* Free the memory that libpq allocated on our behalf */
	PQconninfoFree(start);
}
