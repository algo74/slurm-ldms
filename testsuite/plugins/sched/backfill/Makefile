ifeq ($(OS),Windows_NT)
  ifeq ($(shell uname -s),) # not in a bash-like shell
	CLEANUP = del /F /Q
	MKDIR = mkdir
  else # in a bash-like shell, like msys
	CLEANUP = rm -f
	MKDIR = mkdir -p
  endif
	TARGET_EXTENSION=exe
else
	CLEANUP = rm -Rf
	MKDIR = mkdir -p
	TARGET_EXTENSION=out
endif

.PHONY: clean
.PHONY: test

PATHU = ../../../Unity-master/src/
ROOT = ../../../../
PATHS = $(ROOT)src/plugins/sched/backfill/
PATHC = $(ROOT)src/common/
PATHCO = build/comobjs/
PATHCtrld = $(ROOT)src/slurmctld/
PATHCtrldO = build/slurmctld/
Ctrld_0 = $(patsubst $(PATHCtrld)%.c,$(PATHCtrldO)%.o,$(filter-out %controller.c,$(wildcard $(PATHCtrld)*.c)))
# PATH
PATHT = ./
PATHB = build/
PATHO = build/objs/
PATHR = build/results/

BUILD_PATHS = $(PATHB) $(PATHCO) $(PATHO) $(PATHR) $(PATHCtrldO)

SRCT = $(wildcard $(PATHT)Test_*.c)

COMMON_C = $(wildcard $(PATHC)*.c)
COMMON_O = $(patsubst $(PATHC)%.c,$(PATHCO)%.o,$(COMMON_C) )

COMPILE=gcc -std=gnu99 -fdata-sections -ffunction-sections -g -c
# -dead_strip is for MacOS 
# TODO: implement -Wl,--gc-sections or -Wl,--as-needed for other systems
LINK=gcc -Wl,-dead_strip
DEPEND=gcc -MM -MG -MF
CFLAGS=-I. -I$(ROOT) -I$(PATHU) -I$(PATHS) -I$(PATHC) -DTEST

RESULTS = $(patsubst $(PATHT)Test_%.c,$(PATHR)Test_%.txt,$(SRCT) )

PASSED = `grep -sh :PASS $(PATHR)*.txt`
FAIL = `grep -sh :FAIL $(PATHR)*.txt`
IGNORE = `grep -sh :IGNORE $(PATHR)*.txt`
INCOMPLETE = `grep -L -- '^-----------------------$$' $(PATHR)*.txt`

test: $(BUILD_PATHS) $(RESULTS)
	@echo "-----------------------\nPASSED:\n-----------------------"
	@echo "$(PASSED)"
	@echo "-----------------------\nIGNORES:\n-----------------------"
	@echo "$(IGNORE)"
	@echo "-----------------------\nFAILURES:\n-----------------------"
	@echo "$(FAIL)"
	@echo "-----------------------\nRUN ERROR:\n-----------------------"
	@echo "$(INCOMPLETE)"

	@echo "\nDONE"



$(PATHR)%.txt: $(PATHB)%.$(TARGET_EXTENSION)
	-./$< > $@ 2>&1

$(PATHB)Test_main.$(TARGET_EXTENSION): $(PATHO)unity.o  $(PATHO)Test_main.o
	$(LINK) -o $@ $^

$(PATHB)Test_remote_estimates.$(TARGET_EXTENSION): $(PATHO)override.oo $(PATHO)unity.o  $(PATHO)Test_remote_estimates.o $(PATHO)remote_estimates.o $(PATHO)cJSON.o $(COMMON_O)
	$(LINK) -o $@ $^

$(PATHR)Test_backfill_configure.txt: $(wildcard test_config/*)

$(PATHB)Test_backfill_configure.$(TARGET_EXTENSION): $(PATHO)unity.o  $(PATHO)Test_backfill_configure.o $(PATHO)backfill_configure.o $(PATHO)cJSON.o $(COMMON_O)
	$(LINK) -o $@ $^

$(PATHB)Test_backfill_licenses.$(TARGET_EXTENSION): $(PATHO)override.oo $(PATHO)unity.o  $(PATHO)Test_backfill_licenses.o $(PATHO)backfill_licenses.o $(PATHO)usage_tracker.o $(COMMON_O) $(Ctrld_0)
	$(LINK) -o $@ $^

# $(PATHB)Test_%.$(TARGET_EXTENSION):: $(PATHO)Test_%.o $(PATHO)%.o $(PATHO)unity.o #$(PATHCO)Test%.d
# 	$(LINK) -o $@ $^


$(PATHO)override.oo:: $(PATHT)override.c $(PATHT)override_internal.h
	$(COMPILE) $(CFLAGS) $< -o $@

.SECONDEXPANSION:
$(PATHO)Test_%.o:: $(PATHT)Test_%.c $$(wildcard $(PATHS)%.h)
	$(COMPILE) $(CFLAGS) $< -o $@

.SECONDEXPANSION:
$(PATHO)%.o:: $(PATHS)%.c $$(wildcard $(PATHS)%.h) $(PATHT)override.h
	$(COMPILE)  -include override.h $(CFLAGS) $< -o $@

$(PATHO)%.o:: $(PATHU)%.c $(PATHU)%.h
	$(COMPILE) $(CFLAGS) $< -o $@

$(PATHCO)%.o:: $(PATHC)%.c
	$(COMPILE) $(CFLAGS) $< -o $@

$(PATHCtrldO)%.o:: $(PATHCtrld)%.c
	$(COMPILE) $(CFLAGS) $< -o $@


# $(PATHB):
# 	$(MKDIR) $(PATHB)

# $(PATHCO):
# 	$(MKDIR) $(PATHCO)

# $(PATHO):
# 	$(MKDIR) $(PATHO)

# $(PATHR):
# 	$(MKDIR) $(PATHR)

$(BUILD_PATHS):
	$(MKDIR) $@


clean:
	$(CLEANUP) $(PATHB)


.PRECIOUS: $(PATHB)Test_%.$(TARGET_EXTENSION)
.PRECIOUS: $(PATHCO)%.o
.PRECIOUS: $(PATHO)%.o
.PRECIOUS: $(PATHR)%.txt