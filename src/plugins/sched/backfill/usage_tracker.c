/*
 * usage_tracker.c
 *
 * works with SLURM
 *
 *  Created on: Feb 19, 2020
 *      Author: alex
 */

#include <time.h>
#include <stdio.h>

#include "src/common/list.h"
#include "src/common/xmalloc.h"
#include "src/common/xassert.h"
#include "usage_tracker.h"

#include "src/common/log.h"
#define log(...) debug3(__VA_ARGS__)


typedef struct ut_int_struct {
  time_t start;
  int value;
  float r_star; //CLP ADDED
  float r_star_bar; //CLP ADDED
  uint32_t node_cnt; //CLP ADDED
} ut_int_item_t;


static void
_delete_item(void *x) {
  ut_int_item_t* item = (ut_int_item_t*) x;
  xfree(item);
}


static ut_int_item_t*
_create_item(time_t start, int value) {
  ut_int_item_t *item = xmalloc(sizeof(ut_int_item_t));
  item->start = start;
  item->value = value;
  item->r_star = 0; //CLP ADDED
  item->r_star_bar = 0; //CLP ADDED
  item->node_cnt = 0; //CLP ADDED
  return item;
}

static ut_int_item_t*
_create_item_(time_t start, int value, float r_star, float r_star_bar) { //CLP ADDED
  ut_int_item_t *item = xmalloc(sizeof(ut_int_item_t));
  item->start = start;
  item->value = value;
  item->r_star = r_star; //CLP ADDED
  item->r_star_bar = r_star_bar; //CLP ADDED
  item->node_cnt = 0; //CLP ADDED
  return item;
}

static ut_int_item_t*
_create_item__(time_t start, int value, uint32_t node_cnt) {
  ut_int_item_t *item = xmalloc(sizeof(ut_int_item_t));
  item->start = start;
  item->value = value;
  item->r_star = 0; //CLP ADDED
  item->r_star_bar = 0; //CLP ADDED
  item->node_cnt = node_cnt; //CLP ADDED
  return item;
}

void
ut_int_add_usage(utracker_int_t ut,
               time_t start, time_t end,
               int usage) {
  xassert(start>0);
  xassert(end>start);
  if (usage == 0)
    // do nothing
    return;

  utiterator_t it = list_iterator_create(ut);
  ut_int_item_t *prev;
  ut_int_item_t *next = list_next(it);
  do {
    prev = next;
    next = list_next(it);
  } while(next && next->start < start);
  int prev_value = prev->value;
  int old_value = prev_value;
  if (!next || next->start > start) {
    // add new item to "split the interval"
    prev_value += usage;
    ut_int_item_t *new_item = _create_item(start, prev_value);
    list_insert(it, new_item);
  }
  if (next && next->value + usage == prev_value) {
    // delete next for optimization
    old_value = next->value;
    ut_int_item_t *deleted_item = list_remove(it);
    xassert(deleted_item == next);
    _delete_item(deleted_item);
    next = list_next(it);
  }
  while(next && next->start < end) {
    old_value = next->value;
    prev_value = (next->value += usage);
    next = list_next(it);
  }
  if (!next || next->start > end) {
    // add new item to "split the interval"
    ut_int_item_t *new_item = _create_item(end, old_value);
    list_insert(it, new_item);
  } else if (next->value == prev_value) {
    xassert(next->start == end);
    // delete next for optimization
    ut_int_item_t *deleted_item = list_remove(it);
    xassert(deleted_item == next);
    _delete_item(deleted_item);
    next = list_next(it);
  }
}


void
ut_int_remove_till_end(utracker_int_t ut,
//                      time_t start, int usage) {
                     time_t start, int usage, uint32_t node_cnt) { //CLP Added
  debug3("%s: usage = %d", __func__, usage); //CLP Added
  xassert(start>0);
  if (usage == 0)
    // do nothing
    return;

  utiterator_t it = list_iterator_create(ut);
  ut_int_item_t *prev;
  ut_int_item_t *next = list_next(it);
  do {
    prev = next;
    next = list_next(it);
  } while(next && next->start < start);
  int prev_value = prev->value;
  if (!next || next->start > start) {
    // add new item to "split the interval"
    prev_value -= usage;
    //ut_int_item_t *new_item = _create_item(start, prev_value);
    ut_int_item_t *new_item = _create_item__(start, prev_value, node_cnt);
    list_insert(it, new_item);
  }
  if (!next) {
    return;
  }
  if (next->value - usage == prev_value) {
    // delete next for optimization
    ut_int_item_t *deleted_item = list_remove(it);
    xassert(deleted_item == next);
    _delete_item(deleted_item);
    next = list_next(it);
  }
  while(next) {
    next->value -= usage;
    next = list_next(it);
  }
}


/**
 * returns the time of the beginning of first interval
 * not earlier than time "after" of duration "duration" in tracker "ut"
 * during which the tracked value is below "max_value"
 * or -1 if no such interval
 */
time_t
ut_int_when_below(utracker_int_t ut,
                   time_t after, time_t duration,
//                   int max_value){
		   int max_value,
                   bitstr_t *bitmap){

  xassert(after>0);
  xassert(duration>0);
  utiterator_t it = list_iterator_create(ut);
  ut_int_item_t *prev;
  ut_int_item_t *next = list_next(it);
  int n_avail = bitmap2node_avail(bitmap); //CLP ADDED
  float r_star_bar = next->r_star_bar; //CLP ADDED
  
  do {
    prev = next;
    next = list_next(it);
    n_avail += prev->node_cnt; //CLP ADDED
  } while(next && next->start < after);
  while(1) {
    //while(prev->value >= max_value) {
    while(prev->value >= (max_value - (n_avail * r_star_bar))) {
      debug3("%s: prev->value = %d, max_value = %d, n_avail * r_star_bar = %.2f", __func__, prev->value, max_value, n_avail * r_star_bar); //CLP ADDED
      if (!next) {
        return(-1);
      }
      prev = next;
      next = list_next(it);
      n_avail += prev->node_cnt; //CLP ADDED
    }
    time_t start = prev->start > after ? prev->start : after;
    time_t end = start + duration;
    while(prev->value < max_value) {
      if (!next || next->start >= end) {
        return start;
      }
      prev = next;
      next = list_next(it);
      n_avail += prev->node_cnt; //CLP ADDED
    }
  }
}


utracker_int_t
ut_int_create(int start_value){
  List list = list_create(_delete_item);
  list_append(list, _create_item((time_t)-1, start_value));
  return list;
}

utracker_int_t
ut_int_create_(int start_value, float r_star, float r_star_bar){ //CLP ADDED
  List list = list_create(_delete_item);
  list_append(list, _create_item_((time_t)-1, start_value, r_star, r_star_bar)); //CLP ADDED
  return list;
}

void
ut_int_destroy(utracker_int_t ut) {
  list_destroy(ut);
}


static void
ut_int_dump_item(ut_int_item_t *item) {
  char buff[32];
  log("%24.24s : %d", ctime_r(&(item->start), buff), item->value);
}


static int
_dump_item(void *item, void *arg) {
  ut_int_dump_item((ut_int_item_t *)item);
  return 1;
}


void
ut_int_dump(utracker_int_t ut) {
  log("--------------------------------");
  list_for_each(ut, _dump_item, NULL);
  log("--------------------------------");
}

