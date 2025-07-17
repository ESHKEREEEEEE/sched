#ifndef SCHED_H
#define SCHED_H

#include "postgres.h"

extern volatile sig_atomic_t got_sigterm;
void die(SIGNAL_ARGS);

#endif  // SCHED_H

