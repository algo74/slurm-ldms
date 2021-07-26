/*****************************************************************************\
 *  job_submit_require_timelimit.c - Force job requests to include time limit
 *****************************************************************************
 *  Copyright (C) 2013 Rensselaer Polytechnic Institute
 *  Written by Daniel M. Weeks.
 *
 *  This file is part of Slurm, a resource management program.
 *  For details, see <https://slurm.schedmd.com/>.
 *  Please also read the included file: DISCLAIMER.
 *
 *  Slurm is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  In addition, as a special exception, the copyright holders give permission
 *  to link the code of portions of this program with the OpenSSL library under
 *  certain conditions as described in each individual source file, and
 *  distribute linked combinations including the two. You must obey the GNU
 *  General Public License in all respects for all of the code used other than
 *  OpenSSL. If you modify file(s) with this exception, you may extend this
 *  exception to your version of the file(s), but you are not obligated to do
 *  so. If you do not wish to do so, delete this exception statement from your
 *  version.  If you delete this exception statement from all source files in
 *  the program, then also delete it here.
 *
 *  Slurm is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with Slurm; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
\*****************************************************************************/
#include <pthread.h>
#include <ctype.h>

#include <slurm/slurm.h>
#include <slurm/slurm_errno.h>

#include "src/slurmctld/slurmctld.h"
#include "src/common/xstring.h"

#include "remote_metrics.h"
#include "client.h"


/*
 * These variables are required by the generic plugin interface.  If they
 * are not found in the plugin, the plugin loader will ignore it.
 *
 * plugin_name - a string giving a human-readable description of the
 * plugin.  There is no maximum length, but the symbol must refer to
 * a valid string.
 *
 * plugin_type - a string suggesting t#include <time.h>he type of the plugin or its
 * applicability to a particular form of data or method of data handling.
 * If the low-level plugin API is used, the contents of this string are
 * unimportant and may be anything.  Slurm uses the higher-level plugin
 * interface which requires this string to be of the form
 *
 *	<application>/<method>
 *
 * where <application> is a description of the intended application of
 * the plugin (e.g., "auth" for Slurm authentication) and <method> is a
 * description of how this plugin satisfies that application.  Slurm will
 * only load authentication plugins if the plugin_type string has a prefix
 * of "auth/".
 *
 * plugin_version - an unsigned 32-bit integer containing the Slurm version
 * (major.minor.micro combined into a single number).
 */

const char plugin_name[] = "Rough lustre utilization plugin";
const char plugin_type[] = "job_submit/lustre_util";
const uint32_t plugin_version = SLURM_VERSION_NUMBER;

static pthread_t remote_metrics_thread = 0;
static pthread_mutex_t lustre_util_thread_flag_mutex = PTHREAD_MUTEX_INITIALIZER;

static const char *VARIETY_ID_ENV_NAME = "LDMS_VARIETY_ID";
static const char *REMOTE_SERVER_ENV_NAME = "VINSNL_SERVER";
static const char *REMOTE_SERVER_STRING = "127.0.0.1:9999";
static int sockfd = -1;
static char *variety_id_server = NULL;
static char *variety_id_port = NULL;



int init( void )
{
  debug2( "=========== Lustre utilization plugin starting ================" );

  /* initializing server address */
  char *server_string = getenv(REMOTE_SERVER_ENV_NAME);
  if (server_string == NULL) {
    debug3("%s: env %s isn't set, using default", __func__, REMOTE_SERVER_ENV_NAME);
    server_string = REMOTE_SERVER_STRING;
  }
  char * colon = xstrstr(server_string, ":");
  if (!colon) {
    error("job_submit_lustre_uitl: malformed sever string: \"%s\"", server_string);
    return SLURM_ERROR;
  }
  variety_id_server = xstrndup(server_string, colon - server_string);
  variety_id_port = xstrdup(colon+1);
  debug3("%s: addr: %s, port: %s", __func__, variety_id_server, variety_id_port);

  slurm_mutex_lock( &lustre_util_thread_flag_mutex );
  if ( remote_metrics_thread ) {
    error("Remote metrics thread already running, not starting another" );
    slurm_mutex_unlock( &lustre_util_thread_flag_mutex );
    return SLURM_ERROR;
  }

  remote_metric_agent_arg_t *args = xmalloc(sizeof(remote_metric_agent_arg_t));
  args->addr = xstrdup(variety_id_server);
  args->port = xstrdup(variety_id_port);

  /* since we do a join on this later we don't make it detached */
  slurm_thread_create(&remote_metrics_thread, remote_metrics_agent, (void *)args);

  slurm_mutex_unlock( &lustre_util_thread_flag_mutex );

  return SLURM_SUCCESS;
}



void fini( void )
{
  slurm_mutex_lock( &lustre_util_thread_flag_mutex );
  if ( remote_metrics_thread ) {
    verbose( "Lustre utilization plugin shutting down" );
    stop_remote_metrics_agent();
    pthread_join(remote_metrics_thread, NULL);
    remote_metrics_thread = 0;
  }
  slurm_mutex_unlock( &lustre_util_thread_flag_mutex );
  debug2( "=========== Lustre utilization plugin finished ================" );
}



/**
 * caller keeps the ownership of param_name and param_value
 */
static void _add_or_update_env_param(job_desc_msg_t *job_desc,
                                     const char *param_name,
                                     const char *param_value)
{
  uint32_t envc = job_desc->env_size;
  char **envv = job_desc->environment;

//  int rc = env_array_overwrite(&(job_desc->environment), param_name,
//      param_value);
//
//  if (!rc) {
//    error("%s: could not update env_array");
//  }

  uint32_t i;

  // check if param_name already exists and update if true
  char * new_str = xstrdup_printf("%s=%s", param_name, param_value);
  char * check_str = xstrdup_printf("%s=", param_name);
  int check_len = strlen(check_str);
  for (i = 0; i < envc; i++) {
    debug3("%s: checking param %i", __func__, i);
    if (0 == strncmp(envv[i], check_str, check_len)) {
      // replace parameter
      debug3("%s: freeing envv[%d]: %s", __func__, i, envv[i]);
      xfree(envv[i]);
      envv[i] = new_str;
      // exit
      xfree(check_str);
      return;
    }
  }
  debug3("%s: param %s not found, creating", __func__, param_name);
  xfree(check_str);

  // add new entry if param_name not found
  char **new_envv = xmalloc((envc+2)*sizeof(char*));
  for (i = 0; i < envc; i++) {
    new_envv[i] = envv[i];
  }
  new_envv[envc] = new_str;
  new_envv[envc+1] = NULL;
  job_desc->environment = new_envv;
  job_desc->env_size = envc+1;
  xfree(envv);
}



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

  // make sure we connected
  if (sockfd <= 0) {
    debug3("%s: connecting to host: %s, port: %s",
        __func__, variety_id_server, variety_id_port);
    sockfd = connect_to_simple_server(variety_id_server, variety_id_port);
  }
  if (sockfd <= 0) {
    error("%s: could not connect to the server for job_submit",
        __func__);
    return NULL;
  }

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
 * Given a license string, return if a license with given name is there
 */
static bool _license_exist(char *licenses, const char *name)
{
  char *token, *last;

  if ((licenses == NULL) || (licenses[0] == '\0')) {
    return false;
  }

  token = strtok_r(licenses, ",;", &last);
  while (token) {
    if (xstrncmp(token, name, strlen(name)) == 0) {
      return true;
    }

    token = strtok_r(NULL, ",;", &last);
  }
  return false;
}



static bool _add_license_to_job_desc(job_desc_msg_t *job_desc,
                                        const char *name,
                                        uint32_t num)
{
  char *new_licenses, *licenses = job_desc->licenses;

  debug3("%s: modifying licenses: %s", __func__, licenses);

  if ((licenses == NULL) || (licenses[0] == '\0')){
    // generate new string
    new_licenses = xstrdup_printf("%s:%u;", name, num);
  } else {
    int len = strlen(licenses);
    if (licenses[len-1] != ';') {
      return false;
    }

    licenses[len-1] = '\0';
    new_licenses = xstrdup_printf("%s,%s:%u;", licenses, name, num);
  }
  job_desc->licenses = new_licenses;
  xfree(licenses);
  debug3("%s: new licenses: %s", __func__, new_licenses);
  return true;
}



static char *_get_variety_id(job_desc_msg_t *job_desc, uint32_t uid)
{
  cJSON *request = cJSON_CreateObject();
  // get the comment field
  char *comment = job_desc->comment;
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
    cJSON_AddStringToObject(request, "script_name", job_desc->script);
    debug3("_get_variety_id: job_desc->script is \"%s\"", job_desc->script);
    debug3("_get_variety_id: job_desc->job_id_str is \"%s\"", job_desc->job_id_str);
    int count = job_desc->argc;
    int i;
    for (i = 0; (i < (size_t)count); i++)
    {
        char * n = job_desc->argv[i];
        if(!n) error("_get_variety_id: job_desc->argv[%d] is NULL", i);
        else debug3("_get_variety_id: job_desc->argv[%d] is \"%s\"", i, n);
    }
//    for (i = 0; (i < (size_t)job_desc->env_size); i++)
//    {
//        char * n = job_desc->environment[i];
//        if(!n) error("environment %d is NULL", i);
//        else debug3("environment %d is \"%s\"", i, n);
//    }
    cJSON *arg_array = cJSON_CreateStringArray(job_desc->argv, job_desc->argc);
    cJSON_AddItemToObject(request, "script_args", arg_array);
  }
  char buf[256];
  sprintf(buf, "%d", job_desc->min_nodes);
  cJSON_AddStringToObject(request, "min_nodes", buf);
  sprintf(buf, "%d", job_desc->max_nodes);
  cJSON_AddStringToObject(request, "max_nodes", buf);
  if (job_desc->user_id) {
    uid = job_desc->user_id;
  }
  sprintf(buf, "%d", uid);
  cJSON_AddStringToObject(request, "UID", buf);
  /*AG TODO: add groupid */
  uint32_t gid = 5;
  sprintf(buf, "%d", gid);
  cJSON_AddStringToObject(request, "GID", buf);
  sprintf(buf, "%u", job_desc->job_id); //CLP ADDED job_id
  cJSON_AddStringToObject(request, "job_id", buf); //CLP ADDED job_id

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



extern int job_submit(job_desc_msg_t *job_desc, uint32_t submit_uid,
		      char **err_msg)
{
	// NOTE: no job id actually exists yet (=NO_VAL)

  // get variety_id
  char *variety_id = _get_variety_id(job_desc, submit_uid);
  if (!variety_id) {
    *err_msg = xstrdup("Error getting variety id. Is the server on?");
    return SLURM_ERROR;
  }

  // store the variety_id in the comment field
  char *comment = job_desc->comment;
  char *new_comment = xstrdup_printf("variety_id=%s;%s", variety_id, comment);
  job_desc->comment = new_comment;
  debug3("New comment is '%s'", job_desc->comment);
  xfree(comment);

  // store variety_id so that compute notes can access it
  _add_or_update_env_param(job_desc, VARIETY_ID_ENV_NAME, variety_id);

  // get usage info from remote (if needed)
  /*AG TODO: implement "if needed" check*/
  cJSON * utilization = _get_job_usage(variety_id);
  if (!utilization) {
    *err_msg = xstrdup("Error getting job utilization. Is the server on?");
    return SLURM_ERROR;
  }

  //// set usage for the job

  cJSON * json_object;
  char *end_num;

  // time_limit
  if (job_desc->time_limit == NO_VAL) {
    json_object = cJSON_GetObjectItem(utilization, "time_limit");
    if (!json_object) {
      debug2("%s: didn't get time_limit from server for variety_id %s",
          __func__, variety_id);
    } else if (!cJSON_IsString(json_object)) {
      error("%s: malformed time_limit from server for variety_id %s",
          __func__, variety_id);
    } else {
      long time_limit = strtol(json_object->valuestring, &end_num, 10);
      if (*end_num != '\0' || time_limit < 0) {
        error("%s: can't understand time_limit from server: %s",
            __func__, json_object->valuestring);
      } else if (time_limit == 0) {
        debug3("%s: got zero time_limit from server for variety_id %s",
          __func__, variety_id);
      } else {
        job_desc->time_limit = time_limit;
      }
    }
  }

  // lustre
  if (!_license_exist(job_desc->licenses, "lustre")) {
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
        if (!_add_license_to_job_desc(job_desc, "lustre", num)) {
          error("%s: can't update licenses: %s",
            __func__, job_desc->licenses);
        }
      }
    }
  }

  cJSON_Delete(utilization);

  xfree(variety_id);


	debug3("exiting %s", __func__);

	return SLURM_SUCCESS;
}



int job_modify(job_desc_msg_t *job_desc, job_record_t *job_ptr,
	       uint32_t submit_uid)
{
	if (job_desc->time_limit == INFINITE) {
		info("Bad replacement time limit for %u", job_desc->job_id);
		return ESLURM_INVALID_TIME_LIMIT;
	}

	return SLURM_SUCCESS;
}
