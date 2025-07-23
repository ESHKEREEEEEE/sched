/* contrib/sched/sched--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION sched" to load this file. \quit

CREATE SCHEMA IF NOT EXISTS sched;

--Table for tasks
CREATE TABLE sched.tasks (
    id serial PRIMARY KEY,
    command text NOT NULL,
    run_at timestamp NOT NULL,
    type text NOT NULL,
    created_at timestamp DEFAULT now(),
    status text DEFAULT 'pending',
    executed_at timestamp,
    error text
);

--Table for permitted shell commands
CREATE TABLE sched.whitelist (
    id serial PRIMARY KEY,
    command text NOT NULL
);

--Task scheduling function
DROP FUNCTION IF EXISTS schedule_task(text, timestamptz, text);
CREATE FUNCTION schedule_task(cmd text, run_at timestamptz, type text)
RETURNS void
AS 'sched', 'schedule_task'
LANGUAGE C STRICT
SECURITY DEFINER;

--Running all pending tasks manually
DROP FUNCTION IF EXISTS run_due_tasks();
CREATE FUNCTION run_due_tasks()
RETURNS integer
AS 'sched', 'run_due_tasks'
LANGUAGE C
SECURITY DEFINER;

REVOKE ALL ON sched.tasks FROM PUBLIC;
REVOKE ALL ON sched.whitelist FROM PUBLIC;
GRANT EXECUTE ON FUNCTION schedule_task(text, timestamptz, text) TO PUBLIC;
GRANT USAGE ON SCHEMA sched TO PUBLIC;



