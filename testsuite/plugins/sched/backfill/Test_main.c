#include "unity.h"

void setUp(void) {}
void tearDown(void) {}

void test_Assert(void) {
  TEST_ASSERT_MESSAGE(1, "1 is true");
}

void test_AssertEqual(void) {
  TEST_ASSERT_EQUAL_MESSAGE('\0', 0, "\\0 is zero");
}

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_Assert);
  RUN_TEST(test_AssertEqual);
  return UNITY_END();
}