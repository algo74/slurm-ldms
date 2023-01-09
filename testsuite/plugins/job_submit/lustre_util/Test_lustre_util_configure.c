#include <execinfo.h>
#include <signal.h>
#include <stdio.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <utime.h>

#include "src/common/log.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"
#include "src/plugins/job_submit/lustre_util/lustre_util_configure.h"
#include "src/plugins/sched/backfill/cJSON.h"
#include "unity.h"

// INTERNALS

static const char *ENVVAR_FILENAME = "VINSNL_CONFIG";

typedef enum {
  BACKFILL_LICENSES_AWARE,
  BACKFILL_LICENSES_TWO_GROUP  // workload-adaptive
} backfill_licenses_config_t;

// Mocks

static const char *MOCK_STRING = "reset_mock";
static const int MOCK_INT = 9999;

// int _count_config_vinsnl_server = 0;
// int _count_configure_backfill_licenses = 0;
// int _count_configure_total_node_count = 0;

const char *server_name = NULL;
const char *server_port = NULL;

// backfill_licenses_config_t config_mode = MOCK_INT;

// int node_count = MOCK_INT;

// static void _change_string(char **where, char *what) {
//   if (*where) xfree(*where);
//   *where = what ? xstrdup(what) : NULL;
// }

static void _clear_string(const char **where) {
  if (*where) xfree(*where);
  *where = NULL;
}


// void config_vinsnl_server(char *server, char *port)
// {
//   ++_count_config_vinsnl_server;
//   _change_string(&server_name, server);
//   _change_string(&server_port, port);
// }

// backfill_licenses_config_t configure_backfill_licenses(
//     backfill_licenses_config_t config) {
//   ++_count_configure_backfill_licenses;
//   config_mode = config;
//   }

// int configure_total_node_count(int config) {
//   ++_count_configure_total_node_count;
//   node_count = config;
// }

static void _clear_server_strings()
{
  _clear_string(&server_name);
  _clear_string(&server_port);

}

static void _assert_server_not_configured(char *prefix) {
  const size_t N = 1023;
  char buffer[N+1] = {0};
  snprintf(buffer, N, "%s: Server name is not set", prefix);
  TEST_ASSERT_EQUAL_STRING_MESSAGE(NULL, server_name, buffer);
  snprintf(buffer, N, "%s: Server port is not set", prefix);
  TEST_ASSERT_EQUAL_STRING_MESSAGE(NULL, server_port, prefix);
}


static void _assert_nothing_configured(char *prefix) {
  _assert_server_not_configured(prefix);
}

// HELPERS

static int _touch_file(const char *filename)
// https://rosettacode.org/wiki/File_modification_time#C
{
  struct stat foo;
  time_t mtime;
  struct utimbuf new_times;

  if (stat(filename, &foo) < 0) {
    perror(filename);
    return 1;
  }
  mtime = foo.st_mtime; /* seconds since the epoch */

  new_times.actime = foo.st_atime; /* keep atime unchanged */
  new_times.modtime = time(NULL);  /* set mtime to current time */
  if (utime(filename, &new_times) < 0) {
    perror(filename);
    return 1;
  }
  return 0;
}

static time_t _touch_2_files(const char *filename1, const char *filename2)
{
  time_t mtime;
  struct utimbuf new_times;

  mtime = time(NULL); /* seconds since the epoch */

  new_times.actime = mtime; 
  new_times.modtime = mtime;  
  if (utime(filename1, &new_times) < 0) {
    perror(filename1);
    return -1;
  }
  if (utime(filename2, &new_times) < 0) {
    perror(filename2);
    return -1;
  }
  return mtime;
}

static int _retouch_file(const char *filename1, time_t mtime)
{
  struct utimbuf new_times;

  new_times.actime = mtime; 
  new_times.modtime = mtime;  
  if (utime(filename1, &new_times) < 0) {
    perror(filename1);
    return 1;
  }
  return 0;
}

// GENERAL

void setUp(void) {
  fflush(stderr);
  fflush(stdout);
  unsetenv(ENVVAR_FILENAME);
  _clear_server_strings();
}

void tearDown(void) {
  fflush(stderr);
  fflush(stdout);
  setenv(ENVVAR_FILENAME, "test_config/null_server.json", 1);
  lustre_util_configure();
}

static void handler(int sig)
{
  void *array[10];
  size_t size;

  // get void*'s for all entries on the stack
  size = backtrace(array, 10);

  // print out all the frames to stderr
  fprintf(stderr, "Error: signal %d:\n", sig);
  backtrace_symbols_fd(array, size, STDERR_FILENO);
  exit(1);
}

// TESTS

void test_no_env_variable() {
  _assert_server_not_configured("Init");
  unsetenv(ENVVAR_FILENAME);
  update_and_get_server_address(&server_name, &server_port);
  _assert_nothing_configured("After configure");
}

void test_missing_config_file() {
  _assert_server_not_configured("Init");
  setenv(ENVVAR_FILENAME, "test_config/nonexisting.file", 1);
  update_and_get_server_address(&server_name, &server_port);
    _assert_nothing_configured("After configure");

}

void test_empty_config_file() {
  _assert_server_not_configured("Init");
  setenv(ENVVAR_FILENAME, "test_config/empty.txt", 1);
  update_and_get_server_address(&server_name, &server_port);
    _assert_nothing_configured("After configure");
}

void test_empty_json() {
  _assert_server_not_configured("Init");
  setenv(ENVVAR_FILENAME, "test_config/empty.json", 1);
  update_and_get_server_address(&server_name, &server_port);
    _assert_nothing_configured("After configure");
}

void test_error_json() {
  _assert_server_not_configured("Init");
  setenv(ENVVAR_FILENAME, "test_config/error.json", 1);
  update_and_get_server_address(&server_name, &server_port);
    _assert_nothing_configured("After configure");
}

void test_all_json() {
  _assert_server_not_configured("Init");
  setenv(ENVVAR_FILENAME, "test_config/all.json", 1);
  update_and_get_server_address(&server_name, &server_port);
  TEST_ASSERT_EQUAL_STRING_MESSAGE("server.json_name", server_name, "server_name is properly set");
  TEST_ASSERT_EQUAL_STRING_MESSAGE("server.json_port", server_port, "server_port is properly set");
}

void test_null_server() 
// this test should be first
{
  _assert_server_not_configured("Init");
  setenv(ENVVAR_FILENAME, "test_config/null_server.json", 1);
  update_and_get_server_address(&server_name, &server_port);
  TEST_ASSERT_EQUAL_STRING_MESSAGE(NULL, server_name, "server_name is properly reset");
  TEST_ASSERT_EQUAL_STRING_MESSAGE(NULL, server_port, "server_port is properly reset");
}

void test_null_type() {
  _assert_server_not_configured("Init");
  setenv(ENVVAR_FILENAME, "test_config/null_type.json", 1);
  update_and_get_server_address(&server_name, &server_port);
  TEST_ASSERT_EQUAL_STRING_MESSAGE("server.json_name", server_name, "server_name is properly set");
  TEST_ASSERT_EQUAL_STRING_MESSAGE("server.json_port", server_port, "server_port is properly set");
}

void test_null_nodes() {
  _assert_server_not_configured("Init");
  setenv(ENVVAR_FILENAME, "test_config/null_nodes.json", 1);
  update_and_get_server_address(&server_name, &server_port);
  TEST_ASSERT_EQUAL_STRING_MESSAGE("server.json_name", server_name, "server_name is properly set");
  TEST_ASSERT_EQUAL_STRING_MESSAGE("server.json_port", server_port, "server_port is properly set");
}

void test_server() {
  _assert_server_not_configured("Init");
  setenv(ENVVAR_FILENAME, "test_config/server.json", 1);
  update_and_get_server_address(&server_name, &server_port);
  TEST_ASSERT_EQUAL_STRING_MESSAGE("server.json_name", server_name, "server_name is properly set");
  TEST_ASSERT_EQUAL_STRING_MESSAGE("server.json_port", server_port, "server_port is properly set");
}

void test_server_null() {
  _assert_server_not_configured("Init");
  setenv(ENVVAR_FILENAME, "test_config/server_null.json", 1);
  update_and_get_server_address(&server_name, &server_port);
  TEST_ASSERT_EQUAL_STRING_MESSAGE(NULL, server_name, "server_name is properly set");
  TEST_ASSERT_EQUAL_STRING_MESSAGE(NULL, server_port, "server_port is properly set");
}

void test_server_name_only() {
  _assert_server_not_configured("Init");
  setenv(ENVVAR_FILENAME, "test_config/server_name_only.json", 1);
  update_and_get_server_address(&server_name, &server_port);
    _assert_nothing_configured("After configure");
}

void test_server_port_only() {
  _assert_server_not_configured("Init");
  setenv(ENVVAR_FILENAME, "test_config/server_port_only.json", 1);
  update_and_get_server_address(&server_name, &server_port);
    _assert_nothing_configured("After configure");
}

void test_nodes_json() {
  _assert_server_not_configured("Init");
  setenv(ENVVAR_FILENAME, "test_config/nodes.json", 1);
  update_and_get_server_address(&server_name, &server_port);
  _assert_server_not_configured("Init");
}

void test_nodes_string_json() {
  _assert_server_not_configured("Init");
  setenv(ENVVAR_FILENAME, "test_config/nodes_string.json", 1);
  update_and_get_server_address(&server_name, &server_port);
  _assert_server_not_configured("Init");
}

void test_config_reload_json()
// because of no other option, we will make two different files lock the same
// by making there last modified time the same
{
  const char *file1 = "test_config/sametime1.json";
  const char *file2 = "test_config/sametime2.json";
  time_t mtime = _touch_2_files(file1, file2);
  TEST_ASSERT_MESSAGE(mtime > 0, "No errors resetting files times");
  _assert_server_not_configured("Init");
  setenv(ENVVAR_FILENAME, file1, 1);
  update_and_get_server_address(&server_name, &server_port);
  TEST_ASSERT_EQUAL_STRING_MESSAGE("server1.json_name", server_name, "server_name is properly set");
  TEST_ASSERT_EQUAL_STRING_MESSAGE("server1.json_port", server_port, "server_port is properly set");
  // second configure - file not changed
  _clear_server_strings();
  setenv(ENVVAR_FILENAME, file2, 1);
  update_and_get_server_address(&server_name, &server_port);
  TEST_ASSERT_EQUAL_STRING_MESSAGE("server1.json_name", server_name, "Second configure: server_name is unchanged");
  TEST_ASSERT_EQUAL_STRING_MESSAGE("server1.json_port", server_port, "Second configure: server_port is unchange");
  // third configure - newer config file
  _retouch_file(file2, mtime + 1);
  _clear_server_strings();
  update_and_get_server_address(&server_name, &server_port);
  TEST_ASSERT_EQUAL_STRING_MESSAGE("server2.json_name", server_name, "Third configure: server_name is properly set");
  TEST_ASSERT_EQUAL_STRING_MESSAGE("server2.json_port", server_port, "Third configure: server_port is properly set");
}

int main() {
  signal(SIGSEGV, handler);  // install our handler
  log_options_t log_options = {LOG_LEVEL_DEBUG5, LOG_LEVEL_QUIET,
                               LOG_LEVEL_QUIET, 1, 0};
  printf("%d\n", log_init("LOG", log_options, SYSLOG_FACILITY_DAEMON, NULL));
  UNITY_BEGIN();
  RUN_TEST(test_null_server);
  RUN_TEST(test_no_env_variable);
  RUN_TEST(test_missing_config_file);
  RUN_TEST(test_empty_config_file);
  RUN_TEST(test_empty_json);
  RUN_TEST(test_error_json);
  RUN_TEST(test_all_json);
  RUN_TEST(test_nodes_json);
  RUN_TEST(test_nodes_string_json);
  RUN_TEST(test_server);
  RUN_TEST(test_server_null);
  RUN_TEST(test_server_name_only);
  RUN_TEST(test_server_port_only);
  RUN_TEST(test_null_nodes);
  RUN_TEST(test_null_type);
  RUN_TEST(test_config_reload_json);

  return UNITY_END();
}