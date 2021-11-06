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
    debug3("%s: license_entry->name = %s, license_entry->total = %d", __func__, license_entry->name, license_entry->total); //CLP Added
    lt_entry = list_find_first(lt->tracker, _lt_find_lic_name, license_entry->name);
    if (lt_entry) {
      // returning a little
      time_t t = _convert_time_fwd(job_ptr->end_time, lt->resolution);
      //ut_int_remove_till_end(lt_entry->ut, t,license_entry->total);
      ut_int_remove_till_end(lt_entry->ut, t,license_entry->total, job_ptr->node_cnt); //CLP ADDED
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

int sort_int_list(void *x, void *y) //CLP ADDED
{
  float* _x = (float*) x;
  float* _y = (float*) y;
  if(*_x <= *_y)
  {
    return 1;
  } else {
    return -1;
  }

}

float compute_r_star_bar() { //CLP ADDED

  List r_star_list = list_create(NULL);
  List r_list = list_create(NULL); 
  List n_list = list_create(NULL);

  job_record_t *tmp_job_ptr_; 
  ListIterator job_iterator_ = list_iterator_create(job_list);
  unsigned int size = 0;
  while ((tmp_job_ptr_ = list_next(job_iterator_))) {

    if (!IS_JOB_RUNNING(tmp_job_ptr_) &&
        !IS_JOB_SUSPENDED(tmp_job_ptr_))
      continue;
    if (tmp_job_ptr_->license_list == NULL) {
      debug3("%s: %pJ has NULL license list -- skipping",
            __func__, tmp_job_ptr_);
      continue;
    }

    ListIterator j_iter = list_iterator_create(tmp_job_ptr_->license_list);
    licenses_t *license_entry;
    while ((license_entry = list_next(j_iter))) {
      debug3("%s: license_entry->name = %s, license_entry->total = %d, tmp_job_ptr_->node_cnt = %d", __func__, license_entry->name, license_entry->total, tmp_job_ptr_->node_cnt);
      if(strcmp(license_entry->name, "lustre") == 0)
      {
        float* r_star_ptr = (float*) xmalloc (sizeof(float));
        *r_star_ptr = (float) license_entry->total/tmp_job_ptr_->node_cnt;
        uint32_t* r_ptr = (uint32_t*) xmalloc (sizeof(uint32_t));
        *r_ptr = license_entry->total;
        uint32_t* n_ptr = (uint32_t*) xmalloc (sizeof(uint32_t));
        *n_ptr = tmp_job_ptr_->node_cnt;
        list_push(r_star_list, r_star_ptr);
        list_push(r_list, r_ptr);
        list_push(n_list, n_ptr);
        size += 1;   
      }    
    }
    list_iterator_destroy(j_iter);
  }
  list_iterator_destroy(job_iterator_);

  list_sort(r_star_list, sort_int_list);

  ListIterator r_star_iter = list_iterator_create(r_star_list);
  float *r_star_entry;
  float r_star = 0;
  unsigned int pos = 0;
  unsigned int med_pos = (unsigned int) size/2;
  if(size%2 != 0) med_pos += 1; //CLP ADDED 
  while ((pos < med_pos) && (r_star_entry = list_next(r_star_iter))) {
    debug3("%s: r_star_entry = %.2f, ", __func__, *r_star_entry);
    pos += 1;
    r_star = *r_star_entry; 
  }

  ListIterator r_iter = list_iterator_create(r_list);
  ListIterator n_iter = list_iterator_create(n_list);
  uint32_t *r_entry;
  uint32_t *n_entry;
  uint32_t r_sum = 0;
  uint32_t n_sum = 0;
  float r_star_bar = 0;

  while ((r_entry = list_next(r_iter)) && (n_entry = list_next(n_iter))) {
    if(*r_entry <= (*n_entry * r_star))
    {
      r_sum += *r_entry;
      n_sum += *n_entry;
    }
  }
  if(n_sum == 0) r_star_bar = 0;
  else r_star_bar = (float) r_sum/n_sum;
  
  return r_star_bar;
}

void compute_and_set_r_star_bar(lic_tracker_p lt) { //CLP ADDED
  lt_entry_t *lt_entry;
  lt_entry = list_find_first(lt->tracker, _lt_find_lic_name, "lustre");
  if (lt_entry) {
    debug3("%s: license lustre: lt_entry->total = %d", __func__, lt_entry->total);

    ut_compute_and_set_r_star_bar(lt_entry->ut);
    
  } else {
    error("%s: unknown license lustre", __func__);
  }
}

lic_tracker_p
init_lic_tracker(int resolution) {
  licenses_t *license_entry;
  ListIterator iter;
  lic_tracker_p res = NULL;
  job_record_t *tmp_job_ptr;

  float r_star_bar = compute_r_star_bar();

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
      //entry->ut = ut_int_create(license_entry->used > license_entry->r_used
      //    ? license_entry->used : license_entry->r_used);
      entry->ut = ut_int_create_(license_entry->used > license_entry->r_used
          ? license_entry->used : license_entry->r_used, r_star_bar); //CLP ADDED
      debug3("%s: entry->name = %s, entry->total = %d, entry->ut = (MAX(used = %d, r_used = %d), r_star_bar = %.2f", __func__, entry->name, entry->total, license_entry->used, license_entry->r_used, r_star_bar); //CLP Added
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
  //backfill_licenses_test_job(lt, job_ptr, &check);
  backfill_licenses_test_job(lt, job_ptr, &check, NULL); //CLP ADDED
  int res = check != when;
  if (res) {
    debug3("%s: %pJ overlaps; scheduled: %ld, allowed: %ld",
            __func__, job_ptr, when, check);
    dump_lic_tracker(lt);
  }
  return res;
}

//int backfill_licenses_test_job(lic_tracker_p lt, job_record_t *job_ptr, time_t *when){
int backfill_licenses_test_job(lic_tracker_p lt, job_record_t *job_ptr, time_t *when, bitstr_t *avail_bitmap){ //CLP ADDED
  /*AG TODO: implement reservations */
  /*AG FIXME: should probably use job_ptr->min_time if present */
  /*AG TODO: refactor algorithm */
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
      if (license_entry->total == 0) {
        /* we don't want to do any checks
         * but we have to switch status to prevent constant loops */
        status = CONTINUE;
        continue;
      }
      lt_entry = list_find_first(lt->tracker, _lt_find_lic_name, license_entry->name);
      if (lt_entry) {
	debug3("%s: Job %pJ: license %s: lt_entry->total = %d, license_entry->total = %d", __func__, job_ptr, license_entry->name, lt_entry->total, license_entry->total); //CLP Added
        
        curr_start = ut_int_when_below(lt_entry->ut, prev_start, duration,
            //lt_entry->total - license_entry->total + 1);
            lt_entry->total - license_entry->total + 1, avail_bitmap);
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

int bitmap2node_avail (bitstr_t *bitmap) //CLP ADDED
{
	int i, first, last, node_avail;
	//hostlist_t hl;

	if (bitmap == NULL)
		return 0;

	first = bit_ffs(bitmap);
	if (first == -1)
		return 0;

	last  = bit_fls(bitmap);
        node_avail = last - first + 1;
	//hl = hostlist_create(NULL);
	for (i = first; i <= last; i++) {
		if (bit_test(bitmap, i) == 0)
		{
			node_avail -= 1;
			continue;
   		}
		//hostlist_push_host(hl, node_record_table_ptr[i].name);
                debug3("%s: Node %d available", __func__, i);
	}
	//return hl;
        debug3("%s: node_avail = %d", __func__, node_avail);
        return node_avail;

}

#include "cJSON_src.h" //CLP ADDED

/*static char *_get_variety_id(job_record_t *job_ptr) //CLP ADDED
{
  uint32_t uid = 0;

  cJSON *request = cJSON_CreateObject();
  // get the comment field
  char *comment = job_ptr->comment;
  // get user specified name from the comment field
  char *equalchar = xstrchr(comment, '=');
  if (equalchar && xstrncmp(comment, "jobtype", equalchar - comment) == 0) {
    // if user specified a name - use it
    char *semicolon = xstrchr(equalchar+1, ';');
    if (semicolon) {
      // check if all characters are alphanumeric or '_'
      char *c;
      for(c = equalchar+1; c < semicolon; c++) {
        if (!isalnum(c) && *c != '_') {
          // a wrong character in jobname
          error("_get_variety_id: wrong character in jobtype: '%c'", *c);
          return NULL;
        }
      }
      int len = semicolon-equalchar;
      char *jobname = xstrndup(equalchar+1, len-1);
      debug3("_get_variety_id: Job type is '%s'", jobname);
      // prepare request for jobtype option
      cJSON_AddStringToObject(request, "type", "variety_id/manual");
      cJSON_AddStringToObject(request, "variety_name", jobname);
    } else {
      error("_get_variety_id: no semicolon after jobtype");
          return NULL;
    }
  } else {
    // no job type specified
    // prepare request for auto option
    cJSON_AddStringToObject(request, "type", "variety_id/auto");
    // get script and args
    //cJSON_AddStringToObject(request, "script_name", job_desc->script);
    //cJSON_AddStringToObject(request, "script_name", job_ptr->details->script);
    cJSON_AddStringToObject(request, "script_name", "");
    //debug3("_get_variety_id: job_desc->script is \"%s\"", job_desc->script);
    //debug3("_get_variety_id: job_desc->script is \"%s\"", job_ptr->details->script);
    debug3("_get_variety_id: job_desc->script is \"%s\"", "");
    //debug3("_get_variety_id: job_desc->job_id_str is \"%s\"", job_desc->job_id_str);
    //int count = job_desc->argc;
    int count = job_ptr->details->argc;
    int i;
    for (i = 0; (i < (size_t)count); i++)
    {
        //char * n = job_desc->argv[i];
        char * n = job_ptr->details->argv[i];
        if(!n) error("_get_variety_id: job_desc->argv[%d] is NULL", i);
        else debug3("_get_variety_id: job_desc->argv[%d] is \"%s\"", i, n);
    }
//    for (i = 0; (i < (size_t)job_desc->env_size); i++)
//    {
//        char * n = job_desc->environment[i];
//        if(!n) error("environment %d is NULL", i);
//        else debug3("environment %d is \"%s\"", i, n);
//    }
    //cJSON *arg_array = cJSON_CreateStringArray(job_desc->argv, job_desc->argc);
    cJSON *arg_array = cJSON_CreateStringArray(job_ptr->details->argv, job_ptr->details->argc);
    cJSON_AddItemToObject(request, "script_args", arg_array);
  }
  char buf[256];
  //sprintf(buf, "%d", job_desc->min_nodes);
  sprintf(buf, "%d", job_ptr->details->min_nodes);
  cJSON_AddStringToObject(request, "min_nodes", buf);
  //sprintf(buf, "%d", job_desc->max_nodes);
  sprintf(buf, "%d", job_ptr->details->max_nodes);
  cJSON_AddStringToObject(request, "max_nodes", buf);
  if (job_ptr->user_id) {
    uid = job_ptr->user_id;
  }
  sprintf(buf, "%d", uid);
  cJSON_AddStringToObject(request, "UID", buf);
  //AG TODO: add groupid

  cJSON * resp = _send_receive(request);

  if(resp == NULL){
    error("%s: could not get response from variety_id server", __func__);
    return NULL;
  }

  char *variety_id = NULL;
  cJSON *json_var_id = cJSON_GetObjectItem(resp, "variety_id");
  if (cJSON_IsString(json_var_id)) {
    variety_id = xstrdup(json_var_id->valuestring);
    debug3("Variety id is '%s'", variety_id);
  }
  else {
    error("%s:  malformed response from variety_id server", __func__);
  }
  cJSON_Delete(resp);

  return variety_id;
}
*/

void update_job_usage(job_record_t *job_ptr) { //CLP ADDED
  debug2("%s: Starting update_job_usage", __func__);

  // get variety_id
/*  char *variety_id = _get_variety_id(job_ptr);
  if (!variety_id) {
    debug2("%s: Error getting variety id. Is the server on?", __func__);
    return;
  }

  // get usage info from remote (if needed)
  //AG TODO: implement "if needed" check
  cJSON * utilization = _get_job_usage(variety_id);
  if (!utilization) {
    debug2("%s: Error getting job utilization. Is the server on?", __func__);
    return;
  }

  //// set usage for the job

  cJSON * json_object;
  char *end_num;

  // lustre

  json_object = cJSON_GetObjectItem(utilization, "lustre");
  if (!json_object) {
    debug2("%s: didn't get lustre param from server for variety_id %s",
        __func__, variety_id);
  } else if (!cJSON_IsString(json_object)) {
    error("%s: malformed lustre param from server for variety_id %s",
        __func__, variety_id);
  } else {
    long num = strtol(json_object->valuestring, &end_num, 10);
    if (*end_num != '\0' || num < 0) {
      error("%s: can't understand lustre param from server: %s",
          __func__, json_object->valuestring);
    } else if (num == 0) {
      debug3("%s: got zero lustre param from server for variety_id %s",
        __func__, variety_id);
    } else {
      debug3("%s: MADE IT THIS FAR, num = %ld", num);
	/*
      if (!_add_license_to_job_desc(job_desc, "lustre", num)) {
        error("%s: can't update licenses: %s",
          __func__, job_desc->licenses);
      }
	*/
   /* }
  }
*/
}
