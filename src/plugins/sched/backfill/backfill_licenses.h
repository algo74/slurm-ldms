/*
 * backfill_licenses.h
 *
 *  Created on: Feb 25, 2020
 *      Author: alex
 */

#ifndef SRC_PLUGINS_SCHED_BACKFILL_BACKFILL_LICENSES_H_
#define SRC_PLUGINS_SCHED_BACKFILL_BACKFILL_LICENSES_H_

#include "src/slurmctld/slurmctld.h"

#include "usage_tracker.h"

typedef struct lic_tracker_struct {
  List tracker;
  int resolution;
} lic_tracker_t;

typedef lic_tracker_t *lic_tracker_p;

lic_tracker_p init_lic_tracker(int resolution);

void destroy_lic_tracker(lic_tracker_p);

void dump_lic_tracker(lic_tracker_p lt);

/*
 * Test when the licenses required for a job are available
 * IN job_ptr   - job identification
 * IN/OUT when  - time to check after/when available
 * RET: SLURM_SUCCESS, EAGAIN (not available now), SLURM_ERROR (never runnable)
 */
//int backfill_licenses_test_job(lic_tracker_p lt, job_record_t *job_ptr, time_t *when);
int backfill_licenses_test_job(lic_tracker_p lt, job_record_t *job_ptr, time_t *when, bitstr_t *avail_bitmap); //CLP ADDED

int backfill_licenses_overlap(lic_tracker_p lt, job_record_t *job_ptr, time_t when);

/*
 * Preallocate the licenses required for a just scheduled job
 * IN job_ptr - job identification
 * RET SLURM_SUCCESS or failure code
 */
int backfill_licenses_alloc_job(lic_tracker_p lt, job_record_t *job_ptr,
                                time_t start, time_t end);

int sort_int_list(void *x, void *y); //CLP ADDED

int bitmap2node_avail (bitstr_t *bitmap); //CLP ADDED

float compute_r_star_bar(); //CLP ADDED

//void compute_and_set_r_star_bar(lic_tracker_p lt); //CLP ADDED

#endif /* SRC_PLUGINS_SCHED_BACKFILL_BACKFILL_LICENSES_H_ */
