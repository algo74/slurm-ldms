# Makefile for job_submit/require_timelimit plugin

AUTOMAKE_OPTIONS = foreign

PLUGIN_FLAGS = -module -avoid-version --export-dynamic

AM_CPPFLAGS = -I$(top_srcdir) -I$(top_srcdir)/src/common

pkglib_LTLIBRARIES = job_submit_lustre_util.la

# Job submit plugin.
job_submit_lustre_util_la_SOURCES = job_submit_lustre_util.c \
																		cJSON.h\
																		cJSON.c\
																		client.h\
																		client.c\
																		lustre_util_configure.h\
																		lustre_util_configure.c\
																		remote_metrics.h \
																		remote_metrics.c
job_submit_lustre_util_la_LDFLAGS = $(PLUGIN_FLAGS)
