#ifndef __UNIT_TEST_OVERRIDE_H__
#define __UNIT_TEST_OVERRIDE_H__

#include <unistd.h>

#include "time.h"

#define time(x) unit_test_override_time(x)


time_t unit_test_override_time(time_t *arg);

#define close(x) unit_test_override_close(x)

int unit_test_override_close(int filedes);

#endif  // __UNIT_TEST_OVERRIDE_H__