# Makefile for backfill scheduler plugin

AUTOMAKE_OPTIONS = foreign

AM_CXXFLAGS = -fexceptions

PLUGIN_FLAGS = -module -avoid-version --export-dynamic

AM_CPPFLAGS = -I$(top_srcdir) -I$(top_srcdir)/src/common

pkglib_LTLIBRARIES = sched_backfill.la

sched_backfill_la_SOURCES = backfill_wrapper.c	\
			backfill_configure.c \
			backfill_configure.h \
			backfill_licenses.c \
			backfill_licenses.h \
			backfill.h	\
			backfill.c	\
			cJSON.c	\
			cJSON.h	\
			client.c	\
			client.h	\
			remote_estimates.c	\
			remote_estimates.h	\
			usage_tracker.c \
			usage_tracker.h
sched_backfill_la_LDFLAGS = $(PLUGIN_FLAGS)
