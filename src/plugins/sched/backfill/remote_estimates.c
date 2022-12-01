/*
 * Created by Alex. G on 2022-11-11
 */

// #include "src/common/xmalloc.h"
#include "src/common/xstring.h"
#include "src/slurmctld/licenses.h"

#include "remote_estimates.h"
#include "client.h"

static const char *REMOTE_SERVER_ENV_NAME = "VINSNL_SERVER";
static const char *REMOTE_SERVER_STRING = "127.0.0.1:9999";
static int sockfd = -1;
static char *variety_id_server = NULL;
static char *variety_id_port = NULL;



/**
 * function consumes request
 *
 * caller gets the ownership of response
 */
static cJSON *_send_receive(cJSON* request)
{
  int tries = 0;
  const int max_tries = 3;
RETRY:
  if (++tries > max_tries) {
    error("%s: tried %d times and gave up", __func__, max_tries);
    return NULL;
  }
  // make sure we tried connecting
  if (sockfd <= 0) {
    // make sure we initialized server and port variables
    if (variety_id_server == NULL) {
      char *server_string = getenv(REMOTE_SERVER_ENV_NAME);
      if (server_string == NULL)
      {
        debug3("%s: env %s isn't set, using default", __func__, REMOTE_SERVER_ENV_NAME);
        server_string = REMOTE_SERVER_STRING;
      }
      char *colon = xstrstr(server_string, ":");
      if (!colon)
      {
        error("job_submit_lustre_uitl: malformed sever string: \"%s\"", server_string);
        return NULL;
      }
      variety_id_server = xstrndup(server_string, colon - server_string);
      variety_id_port = xstrdup(colon + 1);
      debug3("%s: addr: %s, port: %s", __func__, variety_id_server, variety_id_port);
    }
    // attempt to connect
    debug3("%s: connecting to host: %s, port: %s",
             __func__, variety_id_server, variety_id_port);
    sockfd = connect_to_simple_server(variety_id_server, variety_id_port);
  }
  // if failed to connect, give up right away
  if (sockfd <= 0) {
    error("%s: could not connect to the server for job_submit",
        __func__);
    return NULL;
  }
  // request response from remote server
  cJSON *resp = send_receive(sockfd, request);
  if (!resp) {
    error("%s: did not get expected response from the server for job_submit",
        __func__);
    close(sockfd);
    sockfd = -1;
    goto RETRY;
  }
  cJSON_Delete(request);
  return resp;
}

/**
 *
 * caller keeps the ownership of the argument and gets the ownership of the result
 */
static cJSON *_get_job_usage(char *variety_id)
{
  cJSON *request = cJSON_CreateObject();
  cJSON_AddStringToObject(request, "type", "job_utilization");
  cJSON_AddStringToObject(request, "variety_id", variety_id);

  cJSON *resp = _send_receive(request);

  if(resp == NULL){
    error("%s: could not get job utilization from server", __func__);
    return NULL;
  }

  cJSON *util = cJSON_GetObjectItem(resp, "response");
  if (util == NULL) {
    error("%s: bad response from server: no response field", __func__);
    return NULL;
  }

  return util;
}

/**
 *
 * caller keeps the ownership of the argument and gets the ownership of the result
 */
char * get_variety_id(job_record_t *job_ptr)
// TODO: DRY in src/slurmctld/job_scheduler.c
{
  static const char pref[] = "variety_id=";
  static const int pref_len = sizeof(pref) - 1;
  char *comment = job_ptr->comment;
  if (xstrncmp(comment, pref, pref_len) == 0)
  {
    char *beginning = comment + pref_len;
    char *end = xstrchr(beginning, ';');
    if (end)
    {
      int len = end - beginning;
      char *variety_id = xstrndup(beginning, len);
      return variety_id;
    }
  }
  return xstrdup("N/A");
}

/*
 * Returns 0 if all good 1 if not timelimit, 2 if no lustre, and 3 if none.
 * Updates results with the obtained utilization estimates.
 * Caller keeps the ownership of all arguments.
 */
int get_variety_id_utilization_from_remote(char *variety_id, remote_estimates_t *results)
{
  int rc = 3; // got nothing so far
  cJSON *utilization = _get_job_usage(variety_id);
  if (!utilization)
  {
    error("%s: Error getting job utilization. Is the server on?", __func__);
    return rc;
  }

  //// set usage for the job
  cJSON *json_object;
  char *end_num;

  // time_limit
  json_object = cJSON_GetObjectItem(utilization, "time_limit");
  if (!json_object)
  {
    debug2("%s: didn't get time_limit from server for variety_id %s",
            __func__, variety_id);
  }
  else if (!cJSON_IsString(json_object))
  {
    error("%s: malformed time_limit from server for variety_id %s",
          __func__, variety_id);
  }
  else
  {
    long time_limit = strtol(json_object->valuestring, &end_num, 10);
    if (*end_num != '\0' || time_limit < 0)
    {
      error("%s: can't understand time_limit from server: %s",
            __func__, json_object->valuestring);
    }
    else if (time_limit == 0)
    {
      debug3("%s: got zero time_limit from server for variety_id %s",
              __func__, variety_id);
    }
    else
    {
      rc = 2; // clear time limit flag
      results->timelimit = time_limit;
    }
  }

  // lustre
  json_object = cJSON_GetObjectItem(utilization, "lustre");
  if (!json_object)
  {
    debug2("%s: didn't get lustre param from server for variety_id %s",
            __func__, variety_id);
  }
  else if (!cJSON_IsString(json_object))
  {
    error("%s: malformed lustre param from server for variety_id %s",
          __func__, variety_id);
  }
  else
  {
    long num = strtol(json_object->valuestring, &end_num, 10);
    if (*end_num != '\0' || num < 0)
    {
      error("%s: can't understand lustre param from server: %s",
            __func__, json_object->valuestring);
    }
    else
    {
      rc &= 1; // clear lustre flag
      results->lustre = num;
    }
  }

  cJSON_Delete(utilization);
  return rc;
}



int get_job_utilization_from_remote(job_record_t *job_ptr, remote_estimates_t *results) 
// docs in the header
{
  debug5("%s: %pJ started", __func__, job_ptr);
  char *variety_id = get_variety_id(job_ptr);
  debug5("%s: %pJ variety id: %s", __func__, job_ptr, variety_id);
  int rc = get_variety_id_utilization_from_remote(variety_id, results);
  debug5("%s: %pJ utilization return code: ", __func__, job_ptr, rc);
  xfree(variety_id);
  debug5("%s: %pJ done", __func__, job_ptr);
  return rc;
}