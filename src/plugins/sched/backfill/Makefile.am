# Makefile for backfill scheduler plugin

AUTOMAKE_OPTIONS = foreign

AM_CXXFLAGS = -fexceptions

PLUGIN_FLAGS = -module -avoid-version --export-dynamic

AM_CPPFLAGS = -I$(top_srcdir) -I$(top_srcdir)/src/common

pkglib_LTLIBRARIES = sched_backfill.la

sched_backfill_la_SOURCES = backfill_wrapper.c	\
			backfill_licenses.h \
			backfill_licenses.c \
			usage_tracker.h \
			usage_tracker.c \
			backfill.c	\
			backfill.h
			#lustre_util.h	\
			#lustre_util.c	\
			#cJSON.h		\
			#cJSON.c		\
			#client.h	\
			#client.c	\
sched_backfill_la_LDFLAGS = $(PLUGIN_FLAGS)
