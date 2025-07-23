// contrib/sched/sched.c

#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "executor/spi.h"
#include "utils/timestamp.h"
#include "utils/lsyscache.h"
#include "utils/elog.h"
#include "sched.h"
#include <sys/wait.h>
#include <stdlib.h>

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(schedule_task);
PG_FUNCTION_INFO_V1(run_due_tasks);

bool exec_sql(char* sql);
int call_shell_command(char* cmd);

volatile sig_atomic_t got_sigterm = false;

void die(SIGNAL_ARGS)
{
    got_sigterm = true;
}

typedef struct {
    int id;
    char *cmd;
    char *type;
} TaskInfo;

typedef struct {
	int id;
	char *cmd;
} WhitelistInfo;

int call_shell_command(char* cmd)
{
	int ret;
	int nrows;
	int spi_status;
	HeapTuple tuple;
    TupleDesc tupdesc;
    Datum cmd_datum;
    bool isnull;
    uint64 i;
	
    ret = -1;
    
    spi_status = SPI_connect();
    if (spi_status != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed");

	//Getting commands from whitelist
    elog(LOG, "[sched:call_shell_command()] Getting whitelist...");
    spi_status = SPI_execute("SELECT id, command FROM sched.whitelist ", false, 0);
    elog(LOG, "[sched:call_shell_command()] SPI_execute status: %d", spi_status);

    if (spi_status != SPI_OK_SELECT)
        elog(ERROR, "SPI_execute failed when selecting tasks");

    if (SPI_tuptable == NULL){ elog(ERROR, "SPI_tuptable is NULL"); }
	
	nrows = SPI_processed;
    tupdesc = SPI_tuptable->tupdesc;
	
    for (i = 0; i < nrows; i++)
    {
        tuple = SPI_tuptable->vals[i];
        cmd_datum = SPI_getbinval(tuple, tupdesc, 2, &isnull);
        if (isnull)
            continue;

        if (!strcmp(cmd, TextDatumGetCString(cmd_datum))) {
        ret = system(cmd); //Call
        
        elog(LOG, "[sched:call_shell_command()] Executed shell command '%s' with result %d", cmd, ret);
        break;
        };
    }
    
	SPI_finish();
    return ret;
}


bool exec_sql(char* sql)
{
    int ret;
    int spi_status;
    
    spi_status = SPI_connect();
    if (spi_status != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed: %d", spi_status);

    ret = SPI_execute(sql, false, 0);  // false = read-only off, 0 = no limit

    if (ret < 0) //Unsuccessful
    {
        SPI_finish();
        elog(ERROR, "SPI_execute failed: %d", ret);
        return false;
    }

    SPI_finish();
	return true;
}

Datum
schedule_task(PG_FUNCTION_ARGS)
{
    text *cmd_text = PG_GETARG_TEXT_PP(0);
    TimestampTz run_at = PG_GETARG_TIMESTAMPTZ(1);
    text *type_text = PG_GETARG_TEXT_PP(2);
    char *cmd = text_to_cstring(cmd_text);
	char *type = text_to_cstring(type_text);
    int ret;
    char sql[1024];

    // Getting run_at in string format
    char *run_at_str = DatumGetCString(DirectFunctionCall1(timestamptz_out, TimestampTzGetDatum(run_at)));

	//Creating sql
    snprintf(sql, sizeof(sql),
             "INSERT INTO sched.tasks(command, run_at, type) VALUES('%s', '%s', '%s')",
             cmd, run_at_str, type);

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
    Datum type_datum;
    bool isnull;
    char update_sql[1024];

    TaskInfo *tasks = NULL;
    int id; 
    char *cmd;
    char *type;
    char *status_str;
    char *error_str;
    int ret = -1;
    
    bool shell_err;

    PG_TRY();
    {
        elog(LOG, "[sched:run_due_tasks()] run_due_tasks() started");

        spi_status = SPI_connect();
        if (spi_status != SPI_OK_CONNECT)
            elog(ERROR, "SPI_connect failed");

		//Getting tasks from sched.tasks
        elog(LOG, "[sched:run_due_tasks()] Selecting due tasks...");
        spi_status = SPI_execute(
            "SELECT id, command, type FROM sched.tasks "
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

        // Saving tasks in TaskInfo struct
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
            type_datum = SPI_getbinval(tuple, tupdesc, 3, &isnull);
			if (isnull)
				continue;
				
            tasks[i].id = DatumGetInt32(id_datum);
            tasks[i].cmd = pstrdup(TextDatumGetCString(cmd_datum)); 
            tasks[i].type = pstrdup(TextDatumGetCString(type_datum)); 
            elog(LOG, "[sched:run_due_tasks()] Added id: %d, %s with type %s", tasks[i].id, tasks[i].cmd, tasks[i].type);
        }

        // Running tasks from TaskInfo
        for (i = 0; i < nrows; i++)
        {
            id = tasks[i].id;
            cmd = tasks[i].cmd;
            type = tasks[i].type;
            
            
			elog(LOG, "[sched] Running task id %d: %s with type: %s", id, cmd, type);
            if (cmd == NULL || strlen(cmd) == 0 || type == NULL || strlen(type) == 0)
            {
            	elog(LOG, "[sched] Found NULL");
                continue;
			}
			
			if (!strcmp(type, "sh"))	//type == sh => shell
			{
				shell_err = 0;
				
				ret = call_shell_command(cmd);
				
				elog(LOG, "[sched:run_due_tasks()] Found command with sh type");
				
				
        		if (ret == -1) 
        		{
    				status_str = "failed";
    				error_str = "system() call failed";
    				shell_err = 1;
				} 
				else if (WIFEXITED(ret)) 
				{
    				int exit_code = WEXITSTATUS(ret);
    				if (exit_code == 0) 
        				status_str = "done";
    				else 
    				{
        				status_str = "failed";
        				error_str = psprintf("exit code %d", exit_code);
        				shell_err = 1;
    				}
				} 
				else if (WIFSIGNALED(ret)) 
				{
    				status_str = "failed";
    				error_str = psprintf("terminated by signal %d", WTERMSIG(ret));
    				shell_err = 1;
				} else 
				{
    				status_str = "failed";
    				error_str = "unknown error";
    				shell_err = 1;
				}
        
				if (shell_err)
				{
				snprintf(update_sql, sizeof(update_sql),
                         	"UPDATE sched.tasks SET status = '%s', executed_at = now(), error = '%s' WHERE id = %d", status_str, error_str, id);
                } 
                else 
                {
                snprintf(update_sql, sizeof(update_sql),
                         	"UPDATE sched.tasks SET status = '%s', executed_at = now() WHERE id = %d", status_str, id);
                }
			}
			else {		//type != sh => sql
            	if (exec_sql(cmd)) //Using exec_sql to run sql tasks
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
            }
            
			//Updating current task status
            elog(LOG, "[sched] Trying to update task %d status", id);
            SPI_execute(update_sql, false, 0);
            elog(LOG, "[sched] Updated task %d status", id);
        }
		elog(LOG, "[sched] Started SPI_finish");
        
        //Freeing memory
        if (tasks)
        {
        	elog(LOG, "[sched] Started pfree(tasks)");
        	
            for (i = 0; i < nrows; i++) 
            {
    			if (tasks[i].cmd)
        			pfree(tasks[i].cmd);
        	}
			pfree(tasks);

            elog(LOG, "[sched] Finished pfree(tasks)");
        }
        SPI_finish();
    }
    PG_CATCH();
    {
        elog(LOG, "[sched:run_due_tasks()] run_due_tasks() catching error...");
        EmitErrorReport();
        FlushErrorState();
        SPI_finish();
        if (tasks)
		{
    	for (i = 0; i < nrows; i++)
    	{
        	if (tasks[i].cmd)
            	pfree(tasks[i].cmd);
        }
    	pfree(tasks);
		}

        PG_RETURN_INT32(task_count);
    }
    PG_END_TRY();

    elog(LOG, "[sched:run_due_tasks()] run_due_tasks() finished, executed %d tasks", task_count);
    PG_RETURN_INT32(task_count);
}



