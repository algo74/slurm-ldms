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
#include "src/plugins/sched/backfill/backfill_configure.h"
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

int _count_config_vinsnl_server = 0;
int _count_configure_backfill_licenses = 0;
int _count_configure_total_node_count = 0;

char *server_name = NULL;
char *server_port = NULL;

backfill_licenses_config_t config_mode = MOCK_INT;

int node_count = MOCK_INT;

static void _change_string(char **where, const char *what) {
  if (*where) xfree(*where);
  *where = what ? xstrdup(what) : NULL;
}


void config_vinsnl_server(char *server, char *port)
{
  ++_count_config_vinsnl_server;
  _change_string(&server_name, server);
  _change_string(&server_port, port);
}

backfill_licenses_config_t configure_backfill_licenses(
    backfill_licenses_config_t config) {
  ++_count_configure_backfill_licenses;
  backfill_licenses_config_t old = config_mode;
  config_mode = config;
  return old;
  }

int configure_total_node_count(int config) {
  ++_count_configure_total_node_count;
  int old = node_count;
  node_count = config;
  return old;
}

static void _clear_server_strings()
{
  _count_config_vinsnl_server = 0;
  _count_configure_backfill_licenses = 0;
  _count_configure_total_node_count = 0;

  _change_string(&server_name, MOCK_STRING);
  _change_string(&server_port, MOCK_STRING);

  config_mode = MOCK_INT;

  node_count = MOCK_INT;
}

static void _assert_server_not_configured() {
  TEST_ASSERT_EQUAL_STRING_MESSAGE(MOCK_STRING, server_name, "Server name is not set");
  TEST_ASSERT_EQUAL_STRING_MESSAGE(MOCK_STRING, server_port, "Server port is not set");
  TEST_ASSERT_EQUAL_INT_MESSAGE(0, _count_config_vinsnl_server, "config_vinsnl_server is not called");
}

static void _assert_config_mode_not_configured() {
  TEST_ASSERT_EQUAL_INT_MESSAGE(MOCK_INT, config_mode, "Backfill config mode is not set");
  TEST_ASSERT_EQUAL_INT_MESSAGE(0, _count_configure_backfill_licenses, "configure_backfill_licenses is not called");
}

static void _assert_node_count_not_configured() {
  TEST_ASSERT_EQUAL_INT_MESSAGE(MOCK_INT, node_count, "Node count is not set");
  TEST_ASSERT_EQUAL_INT_MESSAGE(0, _count_configure_total_node_count, "configure_total_node_count is not called");
}

static void _assert_nothing_configured() {
  _assert_config_mode_not_configured();
  _assert_node_count_not_configured();
  _assert_server_not_configured();
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

// GENERAL

void setUp(void) {
  fflush(stderr);
  fflush(stdout);
  unsetenv(ENVVAR_FILENAME);
  _clear_server_strings();
}

void tearDown(void) {
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
  unsetenv(ENVVAR_FILENAME);
  backfill_configure();
  _assert_nothing_configured();
}

void test_missing_config_file() {
  setenv(ENVVAR_FILENAME, "test_config/nonexisting.file", 1);
  backfill_configure();
  _assert_nothing_configured();
}

void test_empty_config_file() {
  setenv(ENVVAR_FILENAME, "test_config/empty.txt", 1);
  backfill_configure();
  _assert_nothing_configured();
}

void test_empty_json() {
  setenv(ENVVAR_FILENAME, "test_config/empty.json", 1);
  backfill_configure();
  _assert_nothing_configured();
}

void test_error_json() {
  setenv(ENVVAR_FILENAME, "test_config/error.json", 1);
  backfill_configure();
  _assert_nothing_configured();
}

void test_all_json() {
  setenv(ENVVAR_FILENAME, "test_config/all.json", 1);
  backfill_configure();
  TEST_ASSERT_EQUAL_INT_MESSAGE(1, _count_configure_backfill_licenses, "configure_backfill_licenses is called once");
  TEST_ASSERT_EQUAL_INT_MESSAGE(BACKFILL_LICENSES_TWO_GROUP, config_mode, "backfill mode is set to TWO_GROUP");
  TEST_ASSERT_EQUAL_INT_MESSAGE(1, _count_configure_total_node_count, "configure_total_node_count is called once");
  TEST_ASSERT_EQUAL_INT_MESSAGE(777, node_count, "node count is properly set");
  TEST_ASSERT_EQUAL_INT_MESSAGE(1, _count_config_vinsnl_server, "config_vinsnl_server is called once");
  TEST_ASSERT_EQUAL_STRING_MESSAGE("server.json_name", server_name, "server_name is properly set");
  TEST_ASSERT_EQUAL_STRING_MESSAGE("server.json_port", server_port, "server_port is properly set");
}

void test_null_server() {
  setenv(ENVVAR_FILENAME, "test_config/null_server.json", 1);
  backfill_configure();
  TEST_ASSERT_EQUAL_INT_MESSAGE(1, _count_configure_backfill_licenses, "configure_backfill_licenses is called once");
  TEST_ASSERT_EQUAL_INT_MESSAGE(BACKFILL_LICENSES_TWO_GROUP, config_mode, "backfill mode is set to TWO_GROUP");
  TEST_ASSERT_EQUAL_INT_MESSAGE(1, _count_configure_total_node_count, "configure_total_node_count is called once");
  TEST_ASSERT_EQUAL_INT_MESSAGE(777, node_count, "node count is properly set");
  TEST_ASSERT_EQUAL_INT_MESSAGE(1, _count_config_vinsnl_server, "config_vinsnl_server is called once");
  TEST_ASSERT_EQUAL_STRING_MESSAGE(NULL, server_name, "server_name is properly reset");
  TEST_ASSERT_EQUAL_STRING_MESSAGE(NULL, server_port, "server_port is properly reset");
}

void test_null_type() {
  setenv(ENVVAR_FILENAME, "test_config/null_type.json", 1);
  backfill_configure();
  TEST_ASSERT_EQUAL_INT_MESSAGE(1, _count_configure_backfill_licenses, "configure_backfill_licenses is called once");
  TEST_ASSERT_EQUAL_INT_MESSAGE(BACKFILL_LICENSES_AWARE, config_mode, "backfill mode is reset");
  TEST_ASSERT_EQUAL_INT_MESSAGE(1, _count_configure_total_node_count, "configure_total_node_count is called once");
  TEST_ASSERT_EQUAL_INT_MESSAGE(777, node_count, "node count is properly set");
  TEST_ASSERT_EQUAL_INT_MESSAGE(1, _count_config_vinsnl_server, "config_vinsnl_server is called once");
  TEST_ASSERT_EQUAL_STRING_MESSAGE("server.json_name", server_name, "server_name is properly set");
  TEST_ASSERT_EQUAL_STRING_MESSAGE("server.json_port", server_port, "server_port is properly set");
}

void test_null_nodes() {
  setenv(ENVVAR_FILENAME, "test_config/null_nodes.json", 1);
  backfill_configure();
  TEST_ASSERT_EQUAL_INT_MESSAGE(1, _count_configure_backfill_licenses, "configure_backfill_licenses is called once");
  TEST_ASSERT_EQUAL_INT_MESSAGE(BACKFILL_LICENSES_TWO_GROUP, config_mode, "backfill mode is set to TWO_GROUP");
  TEST_ASSERT_EQUAL_INT_MESSAGE(1, _count_configure_total_node_count, "configure_total_node_count is called once");
  TEST_ASSERT_EQUAL_INT_MESSAGE(-1, node_count, "node count is reset");
  TEST_ASSERT_EQUAL_INT_MESSAGE(1, _count_config_vinsnl_server, "config_vinsnl_server is called once");
  TEST_ASSERT_EQUAL_STRING_MESSAGE("server.json_name", server_name, "server_name is properly set");
  TEST_ASSERT_EQUAL_STRING_MESSAGE("server.json_port", server_port, "server_port is properly set");
}

void test_server() {
  setenv(ENVVAR_FILENAME, "test_config/server.json", 1);
  backfill_configure();
  _assert_config_mode_not_configured();
  _assert_node_count_not_configured();
  TEST_ASSERT_EQUAL_INT_MESSAGE(1, _count_config_vinsnl_server, "config_vinsnl_server is called once");
  TEST_ASSERT_EQUAL_STRING_MESSAGE("server.json_name", server_name, "server_name is properly set");
  TEST_ASSERT_EQUAL_STRING_MESSAGE("server.json_port", server_port, "server_port is properly set");
}

void test_server_null() {
  setenv(ENVVAR_FILENAME, "test_config/server_null.json", 1);
  backfill_configure();
  _assert_config_mode_not_configured();
  _assert_node_count_not_configured();
  TEST_ASSERT_EQUAL_INT_MESSAGE(1, _count_config_vinsnl_server, "config_vinsnl_server is called once");
  TEST_ASSERT_EQUAL_STRING_MESSAGE(NULL, server_name, "server_name is properly set");
  TEST_ASSERT_EQUAL_STRING_MESSAGE(NULL, server_port, "server_port is properly set");
}

void test_server_name_only() {
  setenv(ENVVAR_FILENAME, "test_config/server_name_only.json", 1);
  backfill_configure();
  _assert_nothing_configured();
}

void test_server_port_only() {
  setenv(ENVVAR_FILENAME, "test_config/server_port_only.json", 1);
  backfill_configure();
  _assert_nothing_configured();
}

void test_nodes_json() {
  setenv(ENVVAR_FILENAME, "test_config/nodes.json", 1);
  backfill_configure();
  _assert_config_mode_not_configured();
  _assert_server_not_configured();
  TEST_ASSERT_EQUAL_INT_MESSAGE(77, node_count, "node count is properly set");
}

void test_nodes_string_json() {
  setenv(ENVVAR_FILENAME, "test_config/nodes_string.json", 1);
  backfill_configure();
  _assert_config_mode_not_configured();
  _assert_server_not_configured();
  TEST_ASSERT_EQUAL_INT_MESSAGE(MOCK_INT, node_count, "node count is not set");
}

void test_config_reload_json()
{
  const char *filename = "test_config/all.json";
  setenv(ENVVAR_FILENAME, filename, 1);
  backfill_configure();
  TEST_ASSERT_EQUAL_INT_MESSAGE(1, _count_configure_backfill_licenses, "configure_backfill_licenses is called once");
  TEST_ASSERT_EQUAL_INT_MESSAGE(BACKFILL_LICENSES_TWO_GROUP, config_mode, "backfill mode is set to TWO_GROUP");
  TEST_ASSERT_EQUAL_INT_MESSAGE(1, _count_configure_total_node_count, "configure_total_node_count is called once");
  TEST_ASSERT_EQUAL_INT_MESSAGE(777, node_count, "node count is properly set");
  TEST_ASSERT_EQUAL_INT_MESSAGE(1, _count_config_vinsnl_server, "config_vinsnl_server is called once");
  TEST_ASSERT_EQUAL_STRING_MESSAGE("server.json_name", server_name, "server_name is properly set");
  TEST_ASSERT_EQUAL_STRING_MESSAGE("server.json_port", server_port, "server_port is properly set");
  // second configure - file not changed
  _clear_server_strings();
  backfill_configure();
  _assert_nothing_configured();
  // third configure - newer config file
  _touch_file(filename);
  _clear_server_strings();
  backfill_configure();
  TEST_ASSERT_EQUAL_INT_MESSAGE(1, _count_configure_backfill_licenses, "Third configure: configure_backfill_licenses is called once");
  TEST_ASSERT_EQUAL_INT_MESSAGE(BACKFILL_LICENSES_TWO_GROUP, config_mode, "Third configure: backfill mode is set to TWO_GROUP");
  TEST_ASSERT_EQUAL_INT_MESSAGE(1, _count_configure_total_node_count, "Third configure: configure_total_node_count is called once");
  TEST_ASSERT_EQUAL_INT_MESSAGE(777, node_count, "Third configure: node count is properly set");
  TEST_ASSERT_EQUAL_INT_MESSAGE(1, _count_config_vinsnl_server, "Third configure: config_vinsnl_server is called once");
  TEST_ASSERT_EQUAL_STRING_MESSAGE("server.json_name", server_name, "Third configure: server_name is properly set");
  TEST_ASSERT_EQUAL_STRING_MESSAGE("server.json_port", server_port, "Third configure: server_port is properly set");
}

int main() {
  signal(SIGSEGV, handler);  // install our handler
  log_options_t log_options = {LOG_LEVEL_DEBUG5, LOG_LEVEL_QUIET,
                               LOG_LEVEL_QUIET, 1, 0};
  printf("%d\n", log_init("LOG", log_options, SYSLOG_FACILITY_DAEMON, NULL));
  UNITY_BEGIN();
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
  RUN_TEST(test_null_server);
  RUN_TEST(test_null_type);
  RUN_TEST(test_config_reload_json);

  return UNITY_END();
}