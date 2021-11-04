#include <pthread.h>
#include <ctype.h>

#include <slurm/slurm.h>
#include <slurm/slurm_errno.h>

#include "src/slurmctld/slurmctld.h"
#include "src/common/xstring.h"

#include "remote_metrics.h"
#include "client.h"

static cJSON *_send_receive(cJSON* request);
static char *_get_variety_id(job_record_t *job_ptr);
static cJSON *_get_job_usage(char *variety_id);
void update_job_usage(job_record_t *job_ptr);
