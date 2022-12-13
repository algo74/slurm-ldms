/**
 * backfill_licenses.h
 *
 *  Created on: Feb 25, 2020
 *      Author: alex
 */

#ifndef SRC_PLUGINS_SCHED_BACKFILL_BACKFILL_LICENSES_H_
#define SRC_PLUGINS_SCHED_BACKFILL_BACKFILL_LICENSES_H_

#include "src/slurmctld/slurmctld.h"

#include "usage_tracker.h"
#include "remote_estimates.h"

typedef struct lic_tracker_struct {
  List other_licenses;
  List lustre; // this could be either usage_tracker or star_tracker
  int resolution;
  int lustre_offset;
} lic_tracker_t;

typedef lic_tracker_t *lic_tracker_p;

lic_tracker_p init_lic_tracker(int resolution);

void destroy_lic_tracker(lic_tracker_p);

void dump_lic_tracker(lic_tracker_p lt);

/**
 * Test when the licenses required for a job are available
 * IN job_ptr   - job identification
 * IN/OUT when  - time to check after/when available
 * RET: SLURM_SUCCESS, EAGAIN (not available now), SLURM_ERROR (never runnable)
 */
int backfill_licenses_test_job(lic_tracker_p lt, job_record_t *job_ptr, remote_estimates_t *estimates, time_t *when);

int backfill_licenses_overlap(lic_tracker_p lt, job_record_t *job_ptr, remote_estimates_t *estimates, time_t when);

/**
 * Preallocate the licenses required for a just scheduled job
 * IN job_ptr - job identification
 * RET SLURM_SUCCESS or failure code
 */
int backfill_licenses_alloc_job(lic_tracker_p lt, job_record_t *job_ptr, remote_estimates_t *estimates,
                                time_t start, time_t end);

#endif /* SRC_PLUGINS_SCHED_BACKFILL_BACKFILL_LICENSES_H_ */
