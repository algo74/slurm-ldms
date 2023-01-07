#ifndef __UNIT_TEST_OVERRIDE_H__
#define __UNIT_TEST_OVERRIDE_H__

#include <time.h>
#include <unistd.h>

typedef time_t (*time_func_p)(time_t*);

time_t unit_test_override_time(time_t *arg);
time_func_p set_unit_test_override_time(time_func_p new_func);

typedef int (*close_func_p)(int);

int unit_test_override_close(int filedes);
close_func_p set_unit_test_override_close(close_func_p new_func);
void reset_unit_test_override_close();

#endif  // __UNIT_TEST_OVERRIDE_H__