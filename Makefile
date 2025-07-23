# contrib/sched/Makefile

MODULE_big = sched
OBJS = sched.o sched_worker.o

EXTENSION = sched
DATA = sched--1.0.sql
CONTROL = sched.control

REGRESS = run_due_tasks

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/sched
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

