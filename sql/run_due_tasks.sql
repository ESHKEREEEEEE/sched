CREATE EXTENSION sched;

TRUNCATE sched.tasks, sched.whitelist;

INSERT INTO sched.whitelist (command) VALUES ('ls');

INSERT INTO sched.tasks (command, run_at, status, type)
VALUES ('ls', now(), 'pending', 'sh');

SELECT run_due_tasks();

SELECT id, command, status, error IS NULL AS no_error
FROM sched.tasks
ORDER BY id;

