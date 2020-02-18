

#ifndef _SLURM_LUSTRE_UTIL_REMOTE_METRICS_H
#define _SLURM_LUSTRE_UTIL_REMOTE_METRICS_H

/* backfill_agent - detached thread periodically attempts to read remote metrics */
extern void * remote_metrics_agent(void *args);

/* Terminate backfill_agent */
extern void stop_remote_metrics_agent(void);

#endif  /* _SLURM_LUSTRE_UTIL_REMOTE_METRICS_H */
