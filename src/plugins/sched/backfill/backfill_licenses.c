/*
 * backfill_licenses.c
 *
 *  Created on: Feb 25, 2020
 *      Author: alex
 */

#include "backfill_licenses.h"

#include "src/common/xstring.h"
#include "src/slurmctld/licenses.h"
// #include "remote_estimates.h"

#define NOT_IMPLEMENTED 9999

#define LUSTRE "lustre"

extern pthread_mutex_t license_mutex; /* from "src/slurmctld/licenses.c" */

typedef struct lt_entry_struct {
  char *name;
  uint32_t total;
  utracker_int_t ut;
} lt_entry_t;

static time_t _convert_time_floor(time_t t, int resolution) {
  return (t / resolution) * resolution;
}

static time_t _convert_time_fwd(time_t d, int resolution) {
  return (d / resolution + 1) * resolution;
}

void lt_entry_delete(void *x) {
  lt_entry_t *entry = (lt_entry_t *)x;
  if (entry) {
    xfree(entry->name);
    ut_int_destroy(entry->ut);
    xfree(entry);
  }
}

/* AG: copied from src/slurmctld/licenses.c */
/* Find a license_t record by license name (for use by list_find_first) */
static int _license_find_rec(void *x, void *key) {
  licenses_t *license_entry = (licenses_t *)x;
  char *name = (char *)key;

  if ((license_entry->name == NULL) || (name == NULL)) return 0;
  if (xstrcmp(license_entry->name, name)) return 0;
  return 1;
}

/* AG: adapted from get_total_license_cnt from src/slurmctld/licenses.c */
static uint32_t _license_cnt(List licenses_l, char *name) {
  uint32_t count = -1;
  licenses_t *lic;
  if (licenses_l) {
    lic = list_find_first(licenses_l, _license_find_rec, name);
    if (lic) count = lic->total;
  }
  return count;
}

/* Find a lt_entry_t record by license name in license tracker (for use by list_find_first) */
static int _lt_find_lic_name(void *x, void *key) {
  lt_entry_t *entry = (lt_entry_t *)x;
  char *name = (char *)key;

  if ((entry->name == NULL) || (name == NULL)) return 0;
  if (xstrcmp(entry->name, name)) return 0;
  return 1;
}

void dump_lic_tracker(lic_tracker_p lt) {
  ListIterator iter = list_iterator_create(lt->other_licenses);
  lt_entry_t *entry;
  debug3("dumping licenses tracker; resolution: %d", lt->resolution);
  while ((entry = list_next(iter))) {
    debug3("license: %s, total: %d", entry->name, entry->total);
    ut_int_dump(entry->ut);
  }
  list_iterator_destroy(iter);
}

static void _lt_return_single_lic(lic_tracker_p lt, char *name, uint32_t value,
                           job_record_t *job_ptr) {
  lt_entry_t *lt_entry = list_find_first(lt->other_licenses, _lt_find_lic_name, name);
  if (lt_entry) {
    // returning a little
    time_t t = _convert_time_fwd(job_ptr->end_time, lt->resolution);
    ut_int_remove_till_end(lt_entry->ut, t, value);
  } else {
    error("%s: Job %pJ returned unknown license \"%s\"", __func__, job_ptr,
          name);
  }
}

static void _lt_return_lustre(lic_tracker_p lt, int lustre_value,
                       job_record_t *job_ptr) {
  _lt_return_single_lic(lt, LUSTRE, lustre_value, job_ptr);
}

// "returns" licenses used by the job to the license tracker
static void _lt_return_lic(lic_tracker_p lt, job_record_t *job_ptr,
                        remote_estimates_t *estimates) {
  /*AG TODO: implement reservations */
  
  // AG TODO: better way to merge estimates with user data
  int lustre_value = estimates->lustre;
  bool lustre_found = false;
  if (job_ptr->license_list) {
    ListIterator j_iter = list_iterator_create(job_ptr->license_list);
    licenses_t *license_entry;
    lt_entry_t *lt_entry;
    while ((license_entry = list_next(j_iter))) {
      if (xstrcmp(license_entry->name, LUSTRE) == 0) {
        lustre_value = license_entry->total;
        lustre_found = true;
      }
      else
        _lt_return_single_lic(lt, license_entry->name, license_entry->total,
                              job_ptr);
    }
    list_iterator_destroy(j_iter);
    }
  if (lustre_value > 0) {
    _lt_return_lustre(lt, lustre_value, job_ptr);
    if (!lustre_found) {
      lt->lustre_offset += lustre_value;
    }
  }
  debug3("%s: Job %pJ returned licenses", __func__, job_ptr);
  dump_lic_tracker(lt);
}

void destroy_lic_tracker(lic_tracker_p lt) {
  if (lt) {
    list_destroy(lt->other_licenses);
    xfree(lt);
  }
}

lic_tracker_p init_lic_tracker(int resolution) {
  licenses_t *license_entry;
  ListIterator iter;
  lic_tracker_p res = NULL;
  job_record_t *tmp_job_ptr;

  /* create licenses tracker */
  slurm_mutex_lock(&license_mutex);
  if (license_list) {
    res = xmalloc(sizeof(lic_tracker_t));
    res->other_licenses = list_create(lt_entry_delete);
    res->resolution = resolution;
    res->lustre_offset = 0;
    iter = list_iterator_create(license_list);
    while ((license_entry = list_next(iter))) {
      lt_entry_t *entry = xmalloc(sizeof(lt_entry_t));
      entry->name = xstrdup(license_entry->name);
      entry->total = license_entry->total;
      int start_value;
      if (license_entry->used < license_entry->r_used) {
        start_value = license_entry->r_used;
        if (xstrcmp(entry->name, LUSTRE) == 0) {
          res->lustre_offset =
              (int)license_entry->used - (int)license_entry->r_used;
        }
      } else {
        start_value = license_entry->used;
      }
      entry->ut = ut_int_create(start_value);
      list_push(res->other_licenses, entry);
    }
    list_iterator_destroy(iter);
  }
  slurm_mutex_unlock(&license_mutex);

  if (!res) return NULL;

  time_t now = time(NULL);

  /*AG TODO: implement reservations */

  /* process running jobs */
  if(!job_list) {
    return res;
  }
  ListIterator job_iterator = list_iterator_create(job_list);
  while ((tmp_job_ptr = list_next(job_iterator))) {
    if (!IS_JOB_RUNNING(tmp_job_ptr) && !IS_JOB_SUSPENDED(tmp_job_ptr))
      continue;
    time_t end_time = tmp_job_ptr->end_time;
    if (end_time == 0) {
      error("%s: Active %pJ has zero end_time", __func__, tmp_job_ptr);
      continue;
    }
    if (end_time < now) {
      debug3("%s: %pJ might have finished -- yet processing normally", __func__,
             tmp_job_ptr);
    }
    // get estimates
    remote_estimates_t estimates;
    reset_remote_estimates(&estimates);
    get_job_utilization_from_remote(tmp_job_ptr, &estimates);
    if (tmp_job_ptr->license_list == NULL && estimates.lustre == 0) {
      debug3("%s: %pJ has no licenses -- skipping", __func__, tmp_job_ptr);
      continue;
    }
    _lt_return_lic(res, tmp_job_ptr, &estimates);
  }
  list_iterator_destroy(job_iterator);
  // correct lustre offest
  if (res->lustre_offset > 0) {
    lt_entry_t *lt_entry =
        list_find_first(res->other_licenses, _lt_find_lic_name, LUSTRE);
    ut_int_add(lt_entry->ut, res->lustre_offset);
  }

  return res;
}

// TODO
int backfill_licenses_overlap(lic_tracker_p lt, job_record_t *job_ptr,
                              remote_estimates_t *estimates, time_t when) {
  time_t check = when;
  backfill_licenses_test_job(lt, job_ptr, estimates, &check);  // TODO
  int res = check != when;
  if (res) {
    debug3("%s: %pJ overlaps; scheduled: %ld, allowed: %ld", __func__, job_ptr,
           when, check);
    dump_lic_tracker(lt);
  }
  return res;
}

int backfill_licenses_test_job(lic_tracker_p lt, job_record_t *job_ptr,
                               remote_estimates_t *estimates, time_t *when) {
  /*AG TODO: implement reservations */
  /*AG FIXME: should probably use job_ptr->min_time if present */
  /*AG TODO: refactor algorithm */
  /* In case the job is scheduled,
   * we would like the job to fit perfectly if possible.
   * Thus, we will adjust time and duration
   * to what would be used for scheduling. */
  // AG TODO: better way to merge estimates with user data
  int lustre_value = estimates->lustre;
  if (job_ptr->license_list == NULL && lustre_value == 0) {
    debug3("%s: %pJ has no licenses -- skipping", __func__, job_ptr);
    return SLURM_SUCCESS;
  }
  // update lustre_value if explicitly set for the job
  int job_lustre_requirement = _license_cnt(job_ptr->license_list, LUSTRE);
  if (job_lustre_requirement == -1) {
    // using estimate (clipped)
    // TODO (AG): refactor this
    lt_entry_t *lt_entry =
        list_find_first(lt->other_licenses, _lt_find_lic_name, LUSTRE);
    if (lt_entry) {
      if (lustre_value > 0) {
        if (lustre_value >= lt_entry->total) lustre_value = lt_entry->total - 1;
      }
    } else {
      error(
          "%s: Job %pJ is estimated to require lustre which is not in the "
          "licenses list",
          __func__, job_ptr);
      return SLURM_ERROR;
    }
  } else {
    // using provided value
    lustre_value = job_lustre_requirement;
  }
        
  time_t orig_start = _convert_time_floor(*when, lt->resolution);
  time_t duration = _convert_time_fwd(job_ptr->time_limit * 60, lt->resolution);

  int rc = SLURM_SUCCESS;
  ListIterator j_iter = job_ptr->license_list
                            ? list_iterator_create(job_ptr->license_list)
                            : NULL;
  licenses_t *license_entry;
  lt_entry_t *lt_entry;
  time_t curr_start = orig_start;
  time_t prev_start = orig_start;
  enum { FIRST_TIME, RESET, CONTINUE, ERROR } status = RESET;
  while (status == RESET) {
    status = FIRST_TIME;
    debug5("%s: %pJ: inside while loop", __func__, job_ptr);
    if (j_iter) {
      list_iterator_reset(j_iter);
      while ((license_entry = list_next(j_iter))) {
        if (xstrcmp(license_entry->name, LUSTRE) == 0) {
          continue; // lustre is checked separately
        }
        if (license_entry->total == 0) {
          continue;
        }
        lt_entry = list_find_first(lt->other_licenses, _lt_find_lic_name,
                                   license_entry->name);
        if (lt_entry) {
          curr_start =
              ut_int_when_below(lt_entry->ut, prev_start, duration,
                                lt_entry->total - license_entry->total + 1);
          if (curr_start == -1) {
            error("%s: Job %pJ will never get %d license \"%s\"", __func__,
                  job_ptr, license_entry->total, license_entry->name);
            status = ERROR;
            rc = SLURM_ERROR;
            break;
          }
          if (status == FIRST_TIME) {
            prev_start = curr_start;
            status = CONTINUE;
          } else if (curr_start > prev_start) {
            prev_start = curr_start;
            status = RESET;
            break;
          }
        } else {
          error("%s: Job %pJ require unknown license \"%s\"", __func__, job_ptr,
                license_entry->name);
          rc = SLURM_ERROR;
          status = ERROR;
          break;
        }
      }
    }
    debug5("%s: %pJ: done with \"other\" licenses, moving to lustre", __func__, job_ptr);
    if (status != ERROR && status != RESET && lustre_value > 0) {
      lt_entry = list_find_first(lt->other_licenses, _lt_find_lic_name, LUSTRE);
      if (lt_entry) {
        debug5(
            "%s: %pJ: trying lustre ut_int_when_below, prev_start: %ld lustre_value: "
            "%d",
            __func__, job_ptr, prev_start, lustre_value);
        curr_start = ut_int_when_below(lt_entry->ut, prev_start, duration,
                                       lt_entry->total - lustre_value + 1);
        debug5("%s: %pJ: exited lustre ut_int_when_below. Result: %ld", __func__, job_ptr, curr_start);

        if (curr_start == -1) {
          error("%s: Job %pJ will never get %d license \"%s\"",
                __func__, job_ptr, lustre_value, LUSTRE);
          status = ERROR;
          rc = SLURM_ERROR;
        } else if (status == FIRST_TIME) {
          prev_start = curr_start;
          status = CONTINUE;
        } else if (curr_start > prev_start) {
          prev_start = curr_start;
          status = RESET;
        }
      } else {
        error(
            "%s: Job %pJ is estimated to require lustre which is not in the "
            "licenses list",
            __func__, job_ptr);
        rc = SLURM_ERROR;
        status = ERROR;
      }
    }
  }
  if (j_iter) list_iterator_destroy(j_iter);
  /* if the job fits at the requested time, don't update "when"
   * to avoid rounding.
   * Otherwise, update it. */
  if (curr_start != orig_start) {
    *when = curr_start;
  }
  return rc;
}

int backfill_licenses_alloc_job(lic_tracker_p lt, job_record_t *job_ptr,
                                remote_estimates_t *estimates, time_t start,
                                time_t end) {
  /*AG TODO: implement reservations */
  if (job_ptr->license_list == NULL && estimates->lustre == 0) {
    debug3("%s: %pJ has NULL license list -- skipping", __func__, job_ptr);
    return SLURM_SUCCESS;
  }
  int lustre_value = estimates->lustre;
  lt_entry_t *lt_entry;
  start = _convert_time_floor(start, lt->resolution);
  end = _convert_time_fwd(end, lt->resolution);
  if (job_ptr->license_list) {
    ListIterator j_iter = list_iterator_create(job_ptr->license_list);
    licenses_t *license_entry;
    while (NULL != (license_entry = list_next(j_iter))) {
      if (xstrcmp(license_entry->name, LUSTRE) == 0) {
        lustre_value = license_entry->total;
      } else {
        lt_entry =
            list_find_first(lt->other_licenses, _lt_find_lic_name, license_entry->name);
        if (lt_entry) {
          ut_int_add_usage(lt_entry->ut, start, end, license_entry->total);
        } else {
          error("%s: Job %pJ require unknown license \"%s\"", __func__, job_ptr,
                license_entry->name);
        }
      }
    }
    list_iterator_destroy(j_iter);
  }

  if (lustre_value > 0) {
    lt_entry =
        list_find_first(lt->other_licenses, _lt_find_lic_name, LUSTRE);
    if (lt_entry) {
      ut_int_add_usage(lt_entry->ut, start, end, lustre_value);
    } else {
      error("%s: Job %pJ is estimated to use lustre which is not configured", __func__, job_ptr);
    }
  }
  debug3("%s: allocated licenses for %pJ :", __func__, job_ptr);
  dump_lic_tracker(lt);
  return SLURM_SUCCESS;
}
