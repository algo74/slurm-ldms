/*
 * usage_tracker.h
 *
 *  Created on: Feb 19, 2020
 *      Author: alex
 */

#ifndef USAGE_TRACKER_H_
#define USAGE_TRACKER_H_

#include <time.h>

#include "src/common/list.h"
#include "src/common/bitstring.h"

typedef List utracker_int_t;

typedef ListIterator utiterator_t;

void ut_int_add_usage(utracker_int_t ut,
               time_t start, time_t end,
               int value);

time_t ut_int_when_below(utracker_int_t ut,
                   time_t after, time_t duration,
//                   int max_value);
                   int max_value, //CLP ADDED
                   bitstr_t *bitmap); //CLP ADDED

void ut_int_remove_till_end(utracker_int_t ut,
//                      time_t start, int usage);
                        time_t start, int usage, uint32_t node_cnt); //CLP ADDED

utracker_int_t ut_int_create(int start_value);
utracker_int_t ut_int_create_(int start_value, float r_star_bar); //CLP ADDED

void ut_int_destroy(utracker_int_t ut);

void ut_int_dump(utracker_int_t ut);

#endif /* USAGE_TRACKER_H_ */
