// contrib/sched/sched.c

#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "executor/spi.h"
#include "utils/timestamp.h"
#include "utils/lsyscache.h"
#include "utils/elog.h"
#include "sched.h"
#include <stdlib.h>

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(call_shell_command);
PG_FUNCTION_INFO_V1(exec_sql);
PG_FUNCTION_INFO_V1(schedule_task);
PG_FUNCTION_INFO_V1(run_due_tasks);

volatile sig_atomic_t got_sigterm = false;

void die(SIGNAL_ARGS)
{
    got_sigterm = true;
}

typedef struct {
    int id;
    char *cmd;
} TaskInfo;

Datum
call_shell_command(PG_FUNCTION_ARGS)
{
    text *cmd_text = PG_GETARG_TEXT_PP(0);
    
    //TODO: Add whitelist of commands for protection ot che popalo
    
    char *cmd = text_to_cstring(cmd_text);

    int ret = system(cmd);  // Call

    PG_RETURN_INT32(ret);
}

Datum
exec_sql(PG_FUNCTION_ARGS)
{
    text *sql_text = PG_GETARG_TEXT_PP(0);
    char *sql = text_to_cstring(sql_text);

    int ret;
    int spi_status;
    int rows;

    spi_status = SPI_connect();
    if (spi_status != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed: %d", spi_status);

    ret = SPI_execute(sql, false, 0);  // false = read-only off, 0 = no limit

    if (ret != SPI_OK_SELECT && ret != SPI_OK_INSERT &&
        ret != SPI_OK_UPDATE && ret != SPI_OK_DELETE &&
        ret != SPI_OK_INSERT_RETURNING && ret != SPI_OK_UPDATE_RETURNING &&
        ret != SPI_OK_DELETE_RETURNING && ret != SPI_OK_UTILITY)
    {
        SPI_finish();
        elog(ERROR, "SPI_execute failed: %d", ret);
    }

    rows = SPI_processed;
    SPI_finish();

    PG_RETURN_INT32(rows);
}

Datum
schedule_task(PG_FUNCTION_ARGS)
{
    text *cmd_text = PG_GETARG_TEXT_PP(0);
    TimestampTz run_at = PG_GETARG_TIMESTAMPTZ(1);
    char *cmd = text_to_cstring(cmd_text);

    int ret;
    char sql[1024];

    // Getting run_at in string format
    char *run_at_str = DatumGetCString(DirectFunctionCall1(timestamptz_out, TimestampTzGetDatum(run_at)));

	//Creating sql
    snprintf(sql, sizeof(sql),
             "INSERT INTO sched.tasks(command, run_at) VALUES('%s', '%s')",
             cmd, run_at_str);

    ret = SPI_connect();
    if (ret != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed");
	
	//Inserting task into tasks table
    ret = SPI_execute(sql, false, 0);

    SPI_finish();

    if (ret != SPI_OK_INSERT)
        elog(ERROR, "Could not insert task");

    PG_RETURN_VOID();
}

Datum
run_due_tasks(PG_FUNCTION_ARGS)
{
    int spi_status;
    int task_count = 0;
    uint64 nrows = 0;
    uint64 i;
    HeapTuple tuple;
    TupleDesc tupdesc;
    Datum id_datum;
    Datum cmd_datum;
    bool isnull;
    char update_sql[1024];
    int exec_ret;

    TaskInfo *tasks = NULL;
    int id; 
    char *cmd;

    PG_TRY();
    {
        elog(LOG, "[sched:run_due_tasks()] run_due_tasks() started");

        spi_status = SPI_connect();
        if (spi_status != SPI_OK_CONNECT)
            elog(ERROR, "SPI_connect failed");

        elog(LOG, "[sched:run_due_tasks()] Selecting due tasks...");
        spi_status = SPI_execute(
            "SELECT id, command FROM sched.tasks "
            "WHERE run_at <= now() AND status = 'pending'",
            false, 0);

        elog(LOG, "[sched:run_due_tasks()] SPI_execute status: %d", spi_status);

        if (spi_status != SPI_OK_SELECT)
            elog(ERROR, "SPI_execute failed when selecting tasks");

        if (SPI_tuptable == NULL)
            elog(ERROR, "SPI_tuptable is NULL");

        nrows = SPI_processed;
        tupdesc = SPI_tuptable->tupdesc;

        elog(LOG, "[sched:run_due_tasks()] Found %lu pending tasks", (unsigned long)nrows);

        // Копируем задачи
        tasks = (TaskInfo *) palloc0(sizeof(TaskInfo) * nrows);

        for (i = 0; i < nrows; i++)
        {
            tuple = SPI_tuptable->vals[i];

            id_datum = SPI_getbinval(tuple, tupdesc, 1, &isnull);
            if (isnull)
                continue;
            cmd_datum = SPI_getbinval(tuple, tupdesc, 2, &isnull);
            if (isnull)
                continue;

            tasks[i].id = DatumGetInt32(id_datum);
            tasks[i].cmd = TextDatumGetCString(cmd_datum);  // strdup не нужен, уже копия
        }

        // Выполняем задачи
        for (i = 0; i < nrows; i++)
        {
            id = tasks[i].id;
            cmd = tasks[i].cmd;
			elog(LOG, "[sched] Running task id %d: %s", id, cmd);
            if (cmd == NULL || strlen(cmd) == 0)
            {
            	elog(LOG, "[sched] Found NULL");
                continue;
			}
            exec_ret = SPI_execute(cmd, false, 0);

            if (exec_ret == SPI_OK_SELECT || exec_ret == SPI_OK_INSERT ||
                exec_ret == SPI_OK_UPDATE || exec_ret == SPI_OK_DELETE ||
                exec_ret == SPI_OK_INSERT_RETURNING || exec_ret == SPI_OK_UPDATE_RETURNING ||
                exec_ret == SPI_OK_DELETE_RETURNING || exec_ret == SPI_OK_UTILITY)
            {
                snprintf(update_sql, sizeof(update_sql),
                         "UPDATE sched.tasks SET status = 'done', executed_at = now() WHERE id = %d", id);
                elog(LOG, "[sched] Task %d executed successfully", id);
            }
            else
            {
                elog(WARNING, "[sched] Task %d failed to execute: %s", id, cmd);
                snprintf(update_sql, sizeof(update_sql),
                         "UPDATE sched.tasks SET status = 'failed', executed_at = now(), error = 'execution error' WHERE id = %d", id);
            }

            elog(LOG, "[sched] Trying to update task %d status", id);
            SPI_execute(update_sql, false, 0);
            elog(LOG, "[sched] Updated task %d status", id);
        }
		elog(LOG, "[sched] Started SPI_finish");
        SPI_finish();
        
        
        //!!!!!!!!!!!!!!!!!!!!!!!
        //MEMORY LEAK BUT WORKING
        //!!!!!!!!!!!!!!!!!!!!!!!
        /*if (tasks)
        {
        	elog(LOG, "[sched] Started pfree(tasks)");
            pfree(tasks);
            elog(LOG, "[sched] Finished pfree(tasks)");
        }*/
    }
    PG_CATCH();
    {
        elog(LOG, "[sched:run_due_tasks()] run_due_tasks() catching error...");
        EmitErrorReport();
        FlushErrorState();
        SPI_finish();
        if (tasks)
            pfree(tasks);
        PG_RETURN_INT32(task_count);
    }
    PG_END_TRY();

    elog(LOG, "[sched:run_due_tasks()] run_due_tasks() finished, executed %d tasks", task_count);
    PG_RETURN_INT32(task_count);
}



