#include "unity.h"
#include "override_internal.h"

#include "src/plugins/sched/backfill/remote_estimates.h"
#include "src/plugins/sched/backfill/cJSON.h"
char *get_variety_id(job_record_t *job_ptr);
#include "src/slurmctld/slurmctld.h"

// Mocks

int my_close(int fd) {
  info("calling my_close with fd %d", fd);
  return 0;
}

cJSON *response = NULL;
int sockfd_mock = 0;
int count_connect_to_simple_server = 0;
int count_send_recieve = 0;

int connect_to_simple_server(const char *addr, const char *port) {
  count_connect_to_simple_server += 1;
  return sockfd_mock;
}

cJSON *send_receive(int sockfd, cJSON *req) {
  count_send_recieve += 1;
  return response;
}

// HELPERS

static int _fd_is_valid(int fd)
{
  return fcntl(fd, F_GETFD) != -1 || errno != EBADF;
}

void setUp(void) {
  fflush(stderr);
  fflush(stdout);
  response = NULL;
  sockfd_mock = 0;
  count_connect_to_simple_server = 0;
  count_send_recieve = 0;
  config_vinsnl_server("MOCK_host", "MOCK_port");
  set_unit_test_override_close(my_close);
  reset_connection();
}

void tearDown(void) {
  reset_unit_test_override_close();
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

void test_get_job_utilization_from_remote__exits_when_nothing_works_and_not_configured() {
  config_vinsnl_server(NULL, NULL);
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
  for (sockfd_mock = 2; _fd_is_valid(sockfd_mock); ++sockfd_mock);
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

void test_get_job_utilization_from_remote__all_good() {
  sockfd_mock = 2;
  job_record_t job_ptr = {0};
  remote_estimates_t estimates;
  reset_remote_estimates(&estimates);
  response = cJSON_Parse(
      "{"
        "\"status\":\"OK\","
        "\"response\":{"
          "\"lustre\":\"200\","
          "\"time_limit\":\"3\""
        "}"
      "}");
  int rc = get_job_utilization_from_remote(&job_ptr, &estimates);
  TEST_ASSERT_EQUAL_INT_MESSAGE(1, count_send_recieve,
                                "send_recieve called once");
  TEST_ASSERT_EQUAL_INT_MESSAGE(0, rc, "It updates all");
  TEST_ASSERT_EQUAL_INT_MESSAGE(3, estimates.timelimit,
                                "It sets timelimit");
  TEST_ASSERT_EQUAL_INT_MESSAGE(200, estimates.lustre, "It sets Lustre");
}

void test_get_job_utilization_from_remote__disables_on_error_if_not_configured() {
  sockfd_mock = 2;
  job_record_t job_ptr = {0};
  remote_estimates_t estimates;
  reset_remote_estimates(&estimates);
  response = cJSON_Parse(
      "{"
        "\"status\":\"OK\","
        "\"response\":{"
          "\"lustre\":\"200\","
          "\"time_limit\":\"3\""
        "}"
      "}");
  int rc = get_job_utilization_from_remote(&job_ptr, &estimates);
  TEST_ASSERT_EQUAL_INT_MESSAGE(1, count_send_recieve,
                                "send_recieve called once");
  TEST_ASSERT_EQUAL_INT_MESSAGE(0, rc, "It updates all");
  TEST_ASSERT_EQUAL_INT_MESSAGE(3, estimates.timelimit,
                                "It sets timelimit");
  TEST_ASSERT_EQUAL_INT_MESSAGE(200, estimates.lustre, "It sets Lustre");
  // now unconfigure
  config_vinsnl_server(NULL, NULL);
  reset_remote_estimates(&estimates);
  response = cJSON_Parse(
      "{"
        "\"status\":\"OK\","
        "\"response\":{"
          "\"lustre\":\"300\","
          "\"time_limit\":\"4\""
        "}"
      "}");
  rc = get_job_utilization_from_remote(&job_ptr, &estimates);
  TEST_ASSERT_EQUAL_INT_MESSAGE(2, count_send_recieve,
                                "send_recieve called second time");
  TEST_ASSERT_EQUAL_INT_MESSAGE(0, rc, "It updates all");
  TEST_ASSERT_EQUAL_INT_MESSAGE(4, estimates.timelimit,
                                "It sets timelimit");
  TEST_ASSERT_EQUAL_INT_MESSAGE(300, estimates.lustre, "It sets Lustre");
  // now generate error
  reset_remote_estimates(&estimates);
  response = NULL;
  rc = get_job_utilization_from_remote(&job_ptr, &estimates);
  TEST_ASSERT_EQUAL_INT_MESSAGE(3, count_send_recieve,
                                "send_recieve called third time");
  TEST_ASSERT_EQUAL_INT_MESSAGE(3, rc, "Third time, it updates noting");
  TEST_ASSERT_EQUAL_INT_MESSAGE(0, estimates.timelimit,
                                "Third time, timelimit not set");
  TEST_ASSERT_EQUAL_INT_MESSAGE(0, estimates.lustre, "Third time, Lustre not set");
  // now the remote estimates must be disabled
  rc = get_job_utilization_from_remote(&job_ptr, &estimates);
  TEST_ASSERT_EQUAL_INT_MESSAGE(3, count_send_recieve,
                                "Fourth time, send_recieve is not called");
  TEST_ASSERT_EQUAL_INT_MESSAGE(3, rc, "Fourth time, it updates noting");
  TEST_ASSERT_EQUAL_INT_MESSAGE(0, estimates.timelimit,
                                "Fourth time, timelimit not set");
  TEST_ASSERT_EQUAL_INT_MESSAGE(0, estimates.lustre, "Fourth time, Lustre not set");
}

void test_get_job_utilization_from_remote__only_lustre() {
  sockfd_mock = 2;
  job_record_t job_ptr = {0};
  remote_estimates_t estimates;
  reset_remote_estimates(&estimates);
  response = cJSON_Parse(
      "{"
        "\"status\":\"OK\","
        "\"response\":{"
          "\"lustre\":\"200\""
        "}"
      "}");
  int rc = get_job_utilization_from_remote(&job_ptr, &estimates);
  TEST_ASSERT_EQUAL_INT_MESSAGE(1, count_send_recieve,
                                "send_recieve called three times");
  TEST_ASSERT_EQUAL_INT_MESSAGE(1, rc, "no timelimit updated");
  TEST_ASSERT_EQUAL_INT_MESSAGE(0, estimates.timelimit,
                                "It sets timelimit");
  TEST_ASSERT_EQUAL_INT_MESSAGE(200, estimates.lustre, "It sets Lustre");
}

// get_variety_id
////////////////////////////////////////////////////////

void test_get_variety_id__defaults_when_comment_empty() {
  job_record_t job_ptr = {0};
  char *variety_id = get_variety_id(&job_ptr);
  TEST_ASSERT_EQUAL_STRING_MESSAGE(
      "N/A", variety_id,
      "When variety_id is not set, get_variety_id defaults to `N/A`");
}

void test_get_variety_id__defaults_when_comment_malformed() {
  job_record_t job_ptr = {0};
  char *comment = "variety_id=somethingelse";
  job_ptr.comment = comment;
  char *variety_id = get_variety_id(&job_ptr);
  TEST_ASSERT_EQUAL_STRING_MESSAGE(
      "N/A", variety_id,
      "When variety_id is not set, get_variety_id defaults to `N/A`");
}

void test_get_variety_id__defaults_when_comment_malformed2() {
  job_record_t job_ptr = {0};
  char *comment = "comment=somethingelse";
  job_ptr.comment = comment;
  char *variety_id = get_variety_id(&job_ptr);
  TEST_ASSERT_EQUAL_STRING_MESSAGE(
      "N/A", variety_id,
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
  TEST_ASSERT_EQUAL_STRING_MESSAGE("w", variety_id, comment);
}

void test_get_variety_id__single() {
  job_record_t job_ptr = {0};
  char *comment = "variety_id=single;";
  job_ptr.comment = comment;
  char *variety_id = get_variety_id(&job_ptr);
  TEST_ASSERT_EQUAL_STRING_MESSAGE("single", variety_id, comment);
}

int main() {
  // signal(SIGSEGV, handler);  // install our handler
  log_options_t log_options = {LOG_LEVEL_DEBUG5, LOG_LEVEL_QUIET,
                               LOG_LEVEL_QUIET, 1, 0};
  printf("%d\n", log_init("LOG", log_options, SYSLOG_FACILITY_DAEMON, NULL));
  UNITY_BEGIN();
  RUN_TEST(test_get_job_utilization_from_remote__exits_when_nothing_works);
  RUN_TEST(test_get_job_utilization_from_remote__exits_when_send_recieve_fails);
  RUN_TEST(test_get_job_utilization_from_remote__all_good);
  RUN_TEST(test_get_job_utilization_from_remote__only_lustre);
  RUN_TEST(test_get_variety_id__defaults_when_comment_empty);
  RUN_TEST(test_get_variety_id__defaults_when_comment_malformed);
  RUN_TEST(test_get_variety_id__defaults_when_comment_malformed2);
  RUN_TEST(test_get_variety_id__single);
  RUN_TEST(test_get_variety_id__w);
  RUN_TEST(test_get_variety_id__empty);
  RUN_TEST(test_get_job_utilization_from_remote__exits_when_nothing_works_and_not_configured);
  RUN_TEST(test_get_job_utilization_from_remote__disables_on_error_if_not_configured);

  return UNITY_END();
}