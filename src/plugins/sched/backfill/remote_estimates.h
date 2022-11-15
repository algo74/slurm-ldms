/* 
 * Created by Alex G. 2022-11-11
 */
#ifndef SRC_PLUGINS_SCHED_REMOTE_ESTIMATES_H_
#define SRC_PLUGINS_SCHED_REMOTE_ESTIMATES_H_

#include "ctype.h"

#include "slurm/slurm.h"
#include "src/slurmctld/slurmctld.h"
#include "src/common/xmalloc.h"

typedef struct remote_estimates_s
{
  int timelimit;
  int lustre;
} remote_estimates_t;


inline reset_remote_estimates(remote_estimates_t *estimates) {
  estimates->lustre = 0;
  estimates->timelimit = 0;
}


// inline remote_estimates_t *new_remote_estmates()
// {
//   remote_estimates_t *res = xmalloc(sizeof(remote_estimates_t));
//   reset_remote_estimates(res);
//   return res;
// }



/*
  * Returns 0 if all good 1 if not timelimit, 2 if no lustre, and 3 if none.
  * Updates results with the obtained utilization estimates.
  * Caller keeps the ownership of all arguments.
  */
int
get_job_utilization_from_remote(job_record_t *job_ptr, remote_estimates_t *results);

#endif // SRC_PLUGINS_SCHED_REMOTE_ESTIMATES_H_