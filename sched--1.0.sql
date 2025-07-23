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

--Test values for whitelist
--INSERT INTO sched.whitelist(command) VALUES ('ls'); 

--added as sql function for testing
--create function call_shell_command(cmd text)
--RETURNS integer
--AS 'sched', 'call_shell_command'
--LANGUAGE C STRICT;

--Task scheduling function
CREATE FUNCTION schedule_task(cmd text, run_at timestamptz, type text)
RETURNS void
AS 'sched', 'schedule_task'
LANGUAGE C STRICT;

--added as sql function for testing
CREATE FUNCTION run_due_tasks()
RETURNS integer
AS 'sched', 'run_due_tasks'
LANGUAGE C;


