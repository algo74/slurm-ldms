#include "unity.h"

#include "src/plugins/sched/backfill/remote_estimates.h"
#include "src/plugins/sched/backfill/cJSON.h"
char *get_variety_id(job_record_t *job_ptr);
#include "src/slurmctld/slurmctld.h"

// Mocks

cJSON *response = NULL;
int sockfd = 0;
int count_connect_to_simple_server = 0;
int count_send_recieve = 0;

int connect_to_simple_server(const char *addr, const char *port) {
  count_connect_to_simple_server += 1;
  return sockfd;
}

cJSON *send_receive(int sockfd, cJSON *req) {
  count_send_recieve += 1;
  return response;
}

void setUp(void) {
  response = NULL;
  sockfd = 0;
  count_connect_to_simple_server = 0;
  count_send_recieve = 0;
}

void tearDown(void) {
  // nothing
}

// get_job_utilization_from_remote
//////////////////////////////////////////////////////

void test_get_job_utilization_from_remote__exits_when_nothing_works() {
  job_record_t job_ptr = {0};
  remote_estimates_t estimates;
  reset_remote_estimates(&estimates);
  int rc = get_job_utilization_from_remote(&job_ptr, &estimates);
  TEST_ASSERT_EQUAL_INT_MESSAGE(0, count_send_recieve, "send_recieve not called");
  TEST_ASSERT_EQUAL_INT_MESSAGE(3, rc, "no estimates updated");
  TEST_ASSERT_EQUAL_INT_MESSAGE(0, estimates.timelimit, "Time limit not modified");
  TEST_ASSERT_EQUAL_INT_MESSAGE(0, estimates.lustre, "Lustre not modified");
}

void test_get_job_utilization_from_remote__exits_when_send_recieve_fails() {
  sockfd = 2;
  job_record_t job_ptr = {0};
  remote_estimates_t estimates;
  reset_remote_estimates(&estimates);
  int rc = get_job_utilization_from_remote(&job_ptr, &estimates);
  TEST_ASSERT_EQUAL_INT_MESSAGE(3, count_send_recieve,
                                "send_recieve called three times");
  TEST_ASSERT_EQUAL_INT_MESSAGE(3, rc, "no estimates updated");
  TEST_ASSERT_EQUAL_INT_MESSAGE(0, estimates.timelimit,
                                "Time limit not modified");
  TEST_ASSERT_EQUAL_INT_MESSAGE(0, estimates.lustre, "Lustre not modified");
}

// get_variety_id
////////////////////////////////////////////////////////

void test_get_variety_id__defaults_when_comment_empty() {
  job_record_t job_ptr = {0};
  char *variety_id = get_variety_id(&job_ptr);
  TEST_ASSERT_EQUAL_STRING_MESSAGE(variety_id, "N/A", "When variety_id is not set, get_variety_id defaults to `N/A`");
}

void test_get_variety_id__defaults_when_comment_malformed() {
  job_record_t job_ptr = {0};
  char *comment = "variety_id=somethingelse";
  job_ptr.comment = comment;
  char *variety_id = get_variety_id(&job_ptr);
  TEST_ASSERT_EQUAL_STRING_MESSAGE(
      variety_id, "N/A",
      "When variety_id is not set, get_variety_id defaults to `N/A`");
}

void test_get_variety_id__defaults_when_comment_malformed2() {
  job_record_t job_ptr = {0};
  char *comment = "comment=somethingelse";
  job_ptr.comment = comment;
  char *variety_id = get_variety_id(&job_ptr);
  TEST_ASSERT_EQUAL_STRING_MESSAGE(
      variety_id, "N/A",
      "When variety_id is not set, get_variety_id defaults to `N/A`");
}

void test_get_variety_id__empty() {
  job_record_t job_ptr = {0};
  char *comment = "variety_id=;somethingelse";
  job_ptr.comment = comment;
  char *variety_id = get_variety_id(&job_ptr);
  TEST_ASSERT_EQUAL_STRING_MESSAGE(
      variety_id, "",
      comment);
}

void test_get_variety_id__w() {
  job_record_t job_ptr = {0};
  char *comment = "variety_id=w;somethingelse";
  job_ptr.comment = comment;
  char *variety_id = get_variety_id(&job_ptr);
  TEST_ASSERT_EQUAL_STRING_MESSAGE(
      variety_id, "w",
      comment);
}

void test_get_variety_id__single() {
  job_record_t job_ptr = {0};
  char *comment = "variety_id=single;";
  job_ptr.comment = comment;
  char *variety_id = get_variety_id(&job_ptr);
  TEST_ASSERT_EQUAL_STRING_MESSAGE(variety_id, "single", comment);
}

int main() {
  UNITY_BEGIN();
  RUN_TEST(test_get_job_utilization_from_remote__exits_when_nothing_works);
  RUN_TEST(test_get_job_utilization_from_remote__exits_when_send_recieve_fails);
  RUN_TEST(test_get_variety_id__defaults_when_comment_empty);
  RUN_TEST(test_get_variety_id__defaults_when_comment_malformed);
  RUN_TEST(test_get_variety_id__defaults_when_comment_malformed2);
  RUN_TEST(test_get_variety_id__single);
  RUN_TEST(test_get_variety_id__w);
  RUN_TEST(test_get_variety_id__empty);

  return UNITY_END();
}