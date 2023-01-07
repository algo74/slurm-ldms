#include "override_internal.h"

#undef time


time_func_p _unit_test_override_time = time;

time_t unit_test_override_time(time_t *arg) {
  return _unit_test_override_time(arg);
}

time_func_p set_unit_test_override_time(time_func_p new_func) {
  time_func_p old = _unit_test_override_time;
  _unit_test_override_time = new_func;
  return old;
}

#undef close

close_func_p _unit_test_override_close = close;
close_func_p _default_unit_test_override_close = close;

int unit_test_override_close(int filedes) {
  return _unit_test_override_close(filedes);
}

close_func_p set_unit_test_override_close(close_func_p new_func) {
  close_func_p old = _unit_test_override_close;
  _unit_test_override_close = new_func;
  return old;
}

void reset_unit_test_override_close()
{
  _unit_test_override_close = _default_unit_test_override_close;
}