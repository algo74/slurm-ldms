/*
 * remote_metrics.c
 *
 *  Created on: Feb 13, 2020
 *      Author: alex
 */

#include <pthread.h>
#include <stdbool.h>
#include <time.h>
#if HAVE_SYS_PRCTL_H
#  include <sys/prctl.h>
#endif

#include "src/common/slurm_protocol_api.h"
#include "src/common/xstring.h"
#include "src/slurmctld/licenses.h"

#include "client.h"
#include "cJSON.h"

extern List license_list; /*AG from licenses.c */
extern pthread_mutex_t license_mutex; /*AG from licenses.c */

static pthread_mutex_t term_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  term_cond = PTHREAD_COND_INITIALIZER;
static bool stop_remote_metrics = false;

/* Sleep for at least specified time, returns actual sleep time in usec
 *
 * copied from plugins/sched/backfill.c
 * */
static uint32_t _my_sleep(int64_t usec)
{
  int64_t nsec;
  uint32_t sleep_time = 0;
  struct timespec ts = {0, 0};
  struct timeval  tv1 = {0, 0}, tv2 = {0, 0};

  if (gettimeofday(&tv1, NULL)) {   /* Some error */
    sleep(1);
    return 1000000;
  }

  nsec  = tv1.tv_usec + usec;
  nsec *= 1000;
  ts.tv_sec  = tv1.tv_sec + (nsec / 1000000000);
  ts.tv_nsec = nsec % 1000000000;
  slurm_mutex_lock(&term_lock);
  if (!stop_remote_metrics)
    slurm_cond_timedwait(&term_cond, &term_lock, &ts);
  slurm_mutex_unlock(&term_lock);
  if (gettimeofday(&tv2, NULL))
    return usec;
  sleep_time = (tv2.tv_sec - tv1.tv_sec) * 1000000;
  sleep_time += tv2.tv_usec;
  sleep_time -= tv1.tv_usec;
  return sleep_time;
}

/* Find a license_t record by license name (for use by list_find_first)
 *
 * copied from licenses.c
 * */
static int _license_find_rec(void *x, void *key)
{
  licenses_t *license_entry = (licenses_t *) x;
  char *name = (char *) key;

  if ((license_entry->name == NULL) || (name == NULL))
    return 0;
  if (xstrcmp(license_entry->name, name))
    return 0;
  return 1;
}


extern void *remote_metrics_agent(void *args)
{
  int i = 0;
  while(!stop_remote_metrics) {

    // if not connected, attempt to connect

    const char addr[] = "127.0.0.1";
    const char port[] = "2222";

    static int sockfd = 0;

    if (sockfd <= 0) {
      debug3("connecting to lustre remote server");
      sockfd = connect_to_simple_server(addr, port);
    }

    // if connected, get new metrics

    bool updated = false;

    if (sockfd > 0) {
      cJSON *req =  cJSON_CreateObject();
      cJSON_AddStringToObject(req, "type", "usage");
      cJSON_AddStringToObject(req, "request", "lustre");
      cJSON *resp = send_receive(sockfd, req);
      cJSON_Delete(req);
      if (resp) {
        cJSON *payload = cJSON_GetObjectItem(resp, "response");
        // FIXME: check for errors
        i = atoi(payload->valuestring);
        if (i >= 0) {
          updated = true;
        }
      } else {
        debug2("could not connect to Lustre remote server");
        close(sockfd);
        sockfd = -1;
      }
    }

    // if got new metrics, update metrics
    if (updated) {
      licenses_t *match;

      char* license_name = "lustre";

      slurm_mutex_lock(&license_mutex);

      match = list_find_first(license_list, _license_find_rec,
        license_name);
      if (!match) {
        debug2("could not find license %s for update",
              license_name);
      } else {
        match->r_used = i;
        debug3("remotely updated license %s for %d",
                    license_name, i);

      }
      slurm_mutex_unlock(&license_mutex);
    }

    // sleep
    _my_sleep(5 * USEC_IN_SEC);
  }
  return NULL;
}


extern void stop_remote_metrics_agent(void)
{
  slurm_mutex_lock(&term_lock);
  stop_remote_metrics = true;
  slurm_cond_signal(&term_cond);
  slurm_mutex_unlock(&term_lock);
}
