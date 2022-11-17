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
PATHT = ./
PATHB = build/
PATHD = build/depends/
PATHO = build/objs/
PATHR = build/results/

BUILD_PATHS = $(PATHB) $(PATHD) $(PATHO) $(PATHR)

SRCT = $(wildcard $(PATHT)*.c)
COMMON = $(wildcard $(PATHC)*.c)

COMPILE=gcc -fdata-sections -ffunction-sections -c
# -dead_strip is for MacOS 
# TODO: implement -Wl,--gc-sections or -Wl,--as-needed for other systems
LINK=gcc -Wl,-dead_strip
DEPEND=gcc -MM -MG -MF
CFLAGS=-I. -I$(ROOT) -I$(PATHU) -I$(PATHS) -I$(PATHC) -DTEST

RESULTS = $(patsubst $(PATHT)Test_%.c,$(PATHR)Test_%.txt,$(SRCT) )
DEPS = $(patsubst $(PATHC)%.c,$(PATHD)%.d,$(COMMON) )

PASSED = `grep -sh PASS $(PATHR)*.txt`
FAIL = `grep -sh FAIL $(PATHR)*.txt`
IGNORE = `grep -sh IGNORE $(PATHR)*.txt`

test: $(BUILD_PATHS) $(RESULTS)
	@echo "-----------------------\nPASSED:\n-----------------------"
	@echo "$(PASSED)"
	@echo "-----------------------\nIGNORES:\n-----------------------"
	@echo "$(IGNORE)"
	@echo "-----------------------\nFAILURES:\n-----------------------"
	@echo "$(FAIL)"

	@echo "\nDONE"



$(PATHR)%.txt: $(PATHB)%.$(TARGET_EXTENSION)
	-./$< > $@ 2>&1

$(PATHB)Test_main.$(TARGET_EXTENSION): $(PATHO)unity.o  $(PATHO)Test_main.o 
	$(LINK) -o $@ $^

$(PATHB)Test_remote_estimates.$(TARGET_EXTENSION): $(PATHO)unity.o  $(PATHO)Test_remote_estimates.o $(PATHO)remote_estimates.o $(PATHO)cJSON.o $(DEPS)
	$(LINK) -o $@ $^


# $(PATHB)Test_%.$(TARGET_EXTENSION):: $(PATHO)Test_%.o $(PATHO)%.o $(PATHO)unity.o #$(PATHD)Test%.d
# 	$(LINK) -o $@ $^

$(PATHO)Test_%.o:: $(PATHT)Test_%.c $(PATHS)%.h
	$(COMPILE) $(CFLAGS) $< -o $@

$(PATHO)%.o:: $(PATHS)%.c $(PATHS)%.h
	$(COMPILE) $(CFLAGS) $< -o $@

$(PATHO)%.o:: $(PATHC)%.c $(PATHC)%.h
	$(COMPILE) $(CFLAGS) $< -o $@


$(PATHO)%.o:: $(PATHU)%.c $(PATHU)%.h
	$(COMPILE) $(CFLAGS) $< -o $@

$(PATHD)%.d:: $(PATHC)%.c
	$(COMPILE) $(CFLAGS) $< -o $@

$(PATHB):
	$(MKDIR) $(PATHB)

$(PATHD):
	$(MKDIR) $(PATHD)

$(PATHO):
	$(MKDIR) $(PATHO)

$(PATHR):
	$(MKDIR) $(PATHR)



clean:
	$(CLEANUP) $(PATHO)*.o
	$(CLEANUP) $(PATHB)*.$(TARGET_EXTENSION)
	$(CLEANUP) $(PATHR)*.txt
	$(CLEANUP) $(PATHD)*.d

.PRECIOUS: $(PATHB)Test_%.$(TARGET_EXTENSION)
.PRECIOUS: $(PATHD)%.d
.PRECIOUS: $(PATHO)%.o
.PRECIOUS: $(PATHR)%.txt