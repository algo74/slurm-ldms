/*
 * backfill_licenses.c
 *
 *  Created on: Feb 25, 2020
 *      Author: alex
 */

#include "backfill_licenses.h"

#include "src/common/xstring.h"
#include "src/slurmctld/licenses.h"

#define NOT_IMPLEMENTED 9999

extern pthread_mutex_t license_mutex; /* from "src/slurmctld/licenses.c" */

typedef struct lt_entry_struct{
  char *name;
  uint32_t total;
  utracker_int_t ut;
} lt_entry_t;



static time_t _convert_time_floor(time_t t, int resolution) {
  return (t/resolution) * resolution;
}



static time_t _convert_time_fwd(time_t d, int resolution) {
  return (d/resolution + 1) * resolution;
}


void lt_entry_delete(void *x) {
  lt_entry_t *entry = (lt_entry_t*) x;
  if(entry) {
    xfree(entry->name);
    ut_int_destroy(entry->ut);
    xfree(entry);
  }
}



static int _lt_find_lic_name(void *x, void *key) {
  lt_entry_t *entry = (lt_entry_t *) x;
  char *name = (char *) key;

  if ((entry->name == NULL) || (name == NULL))
    return 0;
  if (xstrcmp(entry->name, name))
    return 0;
  return 1;
}



void dump_lic_tracker(lic_tracker_p lt) {
  ListIterator iter = list_iterator_create(lt->tracker);
  lt_entry_t *entry;
  debug3("dumping licenses tracker; resolution: %d", lt->resolution);
  while ((entry = list_next(iter))) {
    debug3("license: %s, total: %d", entry->name, entry->total);
    ut_int_dump(entry->ut);
  }
  list_iterator_destroy(iter);
}



void lt_return_lic(lic_tracker_p lt, job_record_t *job_ptr) {
  /*AG TODO: implement reservations */
  ListIterator j_iter = list_iterator_create(job_ptr->license_list);
  licenses_t *license_entry;
  lt_entry_t *lt_entry;
  while ((license_entry = list_next(j_iter))) {
    lt_entry = list_find_first(lt->tracker, _lt_find_lic_name, license_entry->name);
    if (lt_entry) {
      // returning a little
      time_t t = _convert_time_fwd(job_ptr->end_time, lt->resolution);
      ut_int_remove_till_end(lt_entry->ut, t,license_entry->total);
    } else {
      error("%s: Job %pJ returned unknown license \"%s\"", __func__, job_ptr,
          license_entry->name);
    }
  }
  list_iterator_destroy(j_iter);
  debug3("%s: Job %pJ returned licenses", __func__, job_ptr);
  dump_lic_tracker(lt);
}



void
destroy_lic_tracker(lic_tracker_p lt) {
  list_destroy(lt->tracker);
  xfree(lt);
}



lic_tracker_p
init_lic_tracker(int resolution) {
  licenses_t *license_entry;
  ListIterator iter;
  lic_tracker_p res = NULL;
  job_record_t *tmp_job_ptr;

  /* create licenses tracker */
  slurm_mutex_lock(&license_mutex);
  if (license_list) {
    res = xmalloc(sizeof(lic_tracker_t));
    res->tracker = list_create(lt_entry_delete);
    res->resolution = resolution;
    iter = list_iterator_create(license_list);
    while ((license_entry = list_next(iter))) {
      lt_entry_t *entry = xmalloc(sizeof(lt_entry_t));
      entry->name = xstrdup(license_entry->name);
      entry->total = license_entry->total;
      entry->ut = ut_int_create(license_entry->used > license_entry->r_used
          ? license_entry->used : license_entry->r_used);
      list_push(res->tracker, entry);
    }
    list_iterator_destroy(iter);
  }
  slurm_mutex_unlock(&license_mutex);

  if(!res) return NULL;

  time_t now = time(NULL);

  /*AG TODO: implement reservations */

  /* process running jobs */
  ListIterator job_iterator = list_iterator_create(job_list);
  while ((tmp_job_ptr = list_next(job_iterator))) {
    if (!IS_JOB_RUNNING(tmp_job_ptr) &&
        !IS_JOB_SUSPENDED(tmp_job_ptr))
      continue;
    if (tmp_job_ptr->license_list == NULL) {
      debug3("%s: %pJ has NULL license list -- skipping",
            __func__, tmp_job_ptr);
      continue;
    }
    time_t end_time = tmp_job_ptr->end_time;
    if (end_time == 0) {
      error("%s: Active %pJ has zero end_time",
            __func__, tmp_job_ptr);
      continue;
    }
    if (end_time < now) {
      debug3("%s: %pJ might be finish -- not skipping for now",
            __func__, tmp_job_ptr);
    }
    lt_return_lic(res, tmp_job_ptr);
  }
  list_iterator_destroy(job_iterator);

  return res;
}



int backfill_licenses_overlap(lic_tracker_p lt, job_record_t *job_ptr, time_t when) {
  time_t check = when;
  backfill_licenses_test_job(lt, job_ptr, &check);
  int res = check != when;
  if (res) {
    debug3("%s: %pJ overlaps; scheduled: %ld, allowed: %ld",
            __func__, job_ptr, when, check);
    dump_lic_tracker(lt);
  }
  return res;
}


int backfill_licenses_test_job(lic_tracker_p lt, job_record_t *job_ptr, time_t *when){
  /*AG TODO: implement reservations */
  /*AG FIXME: should probably use job_ptr->min_time if present */
  /* In case the job is scheduled,
   * we would like the job to fit perfectly if possible.
   * Thus, we will adjust time and duration
   * to what would be used for scheduling. */
  time_t orig_start = _convert_time_floor(*when, lt->resolution);
  time_t duration = _convert_time_fwd(job_ptr->time_limit * 60, lt->resolution);
  if (job_ptr->license_list == NULL) {
    debug3("%s: %pJ has NULL license list -- skipping",
           __func__, job_ptr);
    return SLURM_SUCCESS;
  }
  int rc = SLURM_SUCCESS;
  ListIterator j_iter = list_iterator_create(job_ptr->license_list);
  licenses_t *license_entry;
  lt_entry_t *lt_entry;
  time_t curr_start = orig_start;
  time_t prev_start = orig_start;
  enum {
    FIRST_TIME,
    CONTINUE
  } status = FIRST_TIME;
  for (j_iter = list_iterator_create(job_ptr->license_list);
       status != CONTINUE;
       list_iterator_reset(j_iter)) {
    while ((license_entry = list_next(j_iter))) {
      lt_entry = list_find_first(lt->tracker, _lt_find_lic_name, license_entry->name);
      if (lt_entry) {
        curr_start = ut_int_when_below(lt_entry->ut, prev_start, duration,
            lt_entry->total - license_entry->total + 1);
        if (curr_start == -1) {
          error("%s: Job %pJ will never get %d license \"%s\"", __func__, job_ptr,
            license_entry->total, license_entry->name);
          status = CONTINUE;
          rc = SLURM_ERROR;
          break;
        }
        if (status == FIRST_TIME) {
          prev_start = curr_start;
          status = CONTINUE;
        } else if (curr_start > prev_start) {
          prev_start = curr_start;
          status = FIRST_TIME;
          break;
        }
      } else {
        error("%s: Job %pJ require unknown license \"%s\"", __func__, job_ptr,
            license_entry->name);
        rc = SLURM_ERROR;
        status = CONTINUE;
        break;
      }
    }
  }
  list_iterator_destroy(j_iter);
  /* if the job fits at the requested time, don't update "when"
   * to avoid rounding.
   * Otherwise, update it. */
  if (curr_start != orig_start) {
    *when = curr_start;
  }
  return rc;
}



int
backfill_licenses_alloc_job(lic_tracker_p lt,
                            job_record_t *job_ptr,
                            time_t start,
                            time_t end){
  /*AG TODO: implement reservations */
  if (job_ptr->license_list == NULL) {
    debug3("%s: %pJ has NULL license list -- skipping",
          __func__, job_ptr);
    return SLURM_SUCCESS;
  }
  ListIterator j_iter = list_iterator_create(job_ptr->license_list);
  licenses_t *license_entry;
  lt_entry_t *lt_entry;
  start = _convert_time_floor(start, lt->resolution);
  end = _convert_time_fwd(end, lt->resolution);
  while ((license_entry = list_next(j_iter))) {
    lt_entry = list_find_first(lt->tracker, _lt_find_lic_name, license_entry->name);
    if (lt_entry) {
      ut_int_add_usage(lt_entry->ut, start, end, license_entry->total);
    } else {
      error("%s: Job %pJ require unknown license \"%s\"", __func__, job_ptr,
            license_entry->name);
    }
  }
  list_iterator_destroy(j_iter);
  debug3("%s: allocated licenses for %pJ :", __func__, job_ptr);
  dump_lic_tracker(lt);
  return SLURM_SUCCESS;
}
