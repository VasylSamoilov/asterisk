/*
 * SPDX-License-Identifier: GPL-2.0-or-later
 *
 * mbl_map_submit() (chan_mobile -> MAP bridge submit socket) harness.
 * run-tests.py extracts the real function into map_submit.inc, maps the
 * ast_json calls onto jansson, and runs this against the real Python
 * Intake from contrib/mobile-map.
 *
 * usage: map_submit_harness <socket> <number> <body>; exit 0 if accepted.
 */
#define _GNU_SOURCE
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/un.h>
#include <unistd.h>
#include <jansson.h>

#define LOG_WARNING 0
#define ast_log(level, ...) (fprintf(stderr, __VA_ARGS__))
#define ast_verb(level, ...) (fprintf(stderr, __VA_ARGS__))
#define S_OR(a, b) ((a) && *(a) ? (a) : (b))
#define AST_UUID_STR_LEN 37
#define RAII_VAR(type, name, init, fn) type name __attribute__((cleanup(raii_json))) = (init)
#define AST_JSON_OBJECT JSON_OBJECT

static void raii_json(json_t **v) { json_decref(*v); }
#define ast_json json_t
#define ast_json_unref json_decref
#define ast_json_pack json_pack
#define ast_json_dump_string(j) json_dumps((j), JSON_COMPACT)
#define ast_json_free free
#define ast_json_load_string(s, e) json_loads((s), 0, NULL)
#define ast_json_typeof json_typeof
#define ast_json_is_true json_is_true
#define ast_json_object_get json_object_get
#define ast_json_string_get json_string_value

static int generated;
static void ast_uuid_generate_str(char *buf, size_t size)
{
	snprintf(buf, size, "0123abcd-%04x-4567-89ab-cdef01234567", ++generated);
}

static int ast_carefulwrite(int fd, char *s, int len, int timeoutms)
{
	(void) timeoutms;
	while (len > 0) {
		ssize_t w = write(fd, s, len);
		if (w < 0) {
			return -1;
		}
		s += w;
		len -= w;
	}
	return 0;
}

#include "map_submit.inc"

int main(int argc, char **argv)
{
	if (argc != 4) {
		return 2;
	}
	return mbl_map_submit("iphone", argv[1], argv[2], argv[3]) ? 1 : 0;
}
