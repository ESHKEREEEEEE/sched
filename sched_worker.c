// contrib/sched/sched_worker.c

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/procsignal.h"
#include "postmaster/bgworker.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/snapmgr.h"
#include <signal.h>
#include <string.h>


PGDLLEXPORT void sched_worker_main(Datum);

// Flag
static volatile sig_atomic_t got_sigterm = false;

// SIGTERM handler
static void die(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sigterm = true;
    elog(LOG, "[sched_worker] Received SIGTERM");
    SetLatch(MyLatch); // Wake up sleep
    errno = save_errno;
}

PGDLLEXPORT void sched_worker_main(Datum main_arg)
{
	int ret; //SPI_Connect() return value
	int qret;//SPI_Execute() return value
	
    elog(LOG, "[sched_worker] Starting worker");

	//Signal handling
    BackgroundWorkerUnblockSignals();
    pqsignal(SIGTERM, die);

	//DB connection
    elog(LOG, "[sched_worker] Initializing connection to DB...");
    BackgroundWorkerInitializeConnection("postgres", NULL, 0); //DB name "postgres"
    elog(LOG, "[sched_worker] Connected to DB. Entering main loop.");

	//Loop
    while (!got_sigterm)
{
    elog(LOG, "[sched_worker] Starting transaction");

    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());

    elog(LOG, "[sched_worker] Connecting to SPI...");
    ret = SPI_connect();

    if (ret == SPI_OK_CONNECT)
    {
    	//Connected. Executing run_due_tasks();
        elog(LOG, "[sched_worker] SPI connected. Executing task runner...");
        qret = SPI_execute("SELECT run_due_tasks()", false, 0);
        elog(LOG, "[sched_worker] run_due_tasks executed, result = %d", qret);
        SPI_finish();
    }
    else
    {
        elog(WARNING, "[sched_worker] SPI_connect failed with code: %d", ret);
    }
    
    PopActiveSnapshot();
    CommitTransactionCommand();

    elog(LOG, "[sched_worker] Sleeping 10 seconds...");
    pg_usleep(10 * 1000000L);  // Sleeping
    CHECK_FOR_INTERRUPTS();
}

    elog(LOG, "[sched_worker] Exiting cleanly.");
    proc_exit(0);
}

