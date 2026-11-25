/*
 * SPDX-License-Identifier: GPL-2.0-or-later
 *
 * do_sco_listen() harness from asterisk-iphone-bluetooth-gateway-lab by
 * Borisych-C, research/idle-wakeups/tests/mobile/listener_harness.c at
 * commit cf286672631c4788eb37b90ad7d8c7242f67a103 (unmodified below).
 * Run through run-tests.py, which extracts the function into mobile_loop.inc.
 */
#define _GNU_SOURCE
#include <assert.h>
#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

/* This harness compiles the mechanically extracted production function.
 * Only Asterisk's wrapper and adapter state are replaced. Poll and accept
 * operate on a real Unix SOCK_SEQPACKET listener, not simulated readiness.
 */
struct io_context {
	int fd;
	pthread_mutex_t lock;
	pthread_cond_t changed;
	unsigned int waits;
	unsigned int accepts;
	unsigned int interrupts;
	bool gate_first_wait;
	bool gate_open;
	bool inject_fatal_error;
	bool done;
	double thread_cpu;
};

struct adapter_pvt {
	struct io_context *accept_io;
	const char *id;
};

static atomic_bool unloading;
static atomic_uint signals_seen;
static atomic_uint error_logs;
static unsigned int test_number;

static double monotonic_seconds(void)
{
	struct timespec now;
	assert(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
	return now.tv_sec + now.tv_nsec / 1e9;
}

static struct timespec deadline_after(double seconds)
{
	struct timespec end;
	assert(clock_gettime(CLOCK_MONOTONIC, &end) == 0);
	end.tv_sec += (time_t) seconds;
	end.tv_nsec += (long) ((seconds - (time_t) seconds) * 1e9);
	if (end.tv_nsec >= 1000000000L) {
		++end.tv_sec;
		end.tv_nsec -= 1000000000L;
	}
	return end;
}

static void pause_ms(long milliseconds)
{
	struct timespec remaining = {milliseconds / 1000, milliseconds % 1000 * 1000000L};
	while (nanosleep(&remaining, &remaining) != 0) {
		assert(errno == EINTR);
	}
}

static void signal_handler(int signo)
{
	(void) signo;
	atomic_fetch_add_explicit(&signals_seen, 1, memory_order_relaxed);
}

static int check_unloading(void)
{
	return atomic_load_explicit(&unloading, memory_order_acquire);
}

#define LOG_ERROR 1
#define ast_log(level, ...) do { \
	(void) (level); \
	atomic_fetch_add_explicit(&error_logs, 1, memory_order_relaxed); \
} while (0)

static int ast_io_wait(struct io_context *ioc, int timeout)
{
	struct pollfd descriptor = {.fd = ioc->fd, .events = POLLIN};
	int result;
	int saved_errno;
	bool inject_error;
	assert(pthread_mutex_lock(&ioc->lock) == 0);
	++ioc->waits;
	assert(pthread_cond_broadcast(&ioc->changed) == 0);
	while (ioc->gate_first_wait && ioc->waits == 1 && !ioc->gate_open) {
		assert(pthread_cond_wait(&ioc->changed, &ioc->lock) == 0);
	}
	inject_error = ioc->inject_fatal_error;
	assert(pthread_mutex_unlock(&ioc->lock) == 0);
	if (inject_error) {
		errno = EINVAL;
		return -1;
	}

	result = poll(&descriptor, 1, timeout);
	saved_errno = errno;
	if (result < 0 && saved_errno == EINTR) {
		assert(pthread_mutex_lock(&ioc->lock) == 0);
		++ioc->interrupts;
		assert(pthread_cond_broadcast(&ioc->changed) == 0);
		assert(pthread_mutex_unlock(&ioc->lock) == 0);
	}
	if (result > 0) {
		assert(descriptor.revents == POLLIN);
		int accepted = accept(ioc->fd, NULL, NULL);
		assert(accepted >= 0);
		assert(close(accepted) == 0);
		assert(pthread_mutex_lock(&ioc->lock) == 0);
		++ioc->accepts;
		assert(pthread_cond_broadcast(&ioc->changed) == 0);
		assert(pthread_mutex_unlock(&ioc->lock) == 0);
	}
	/* ast_io_wait preserves errno for a <= 0 poll result. */
	errno = saved_errno;
	return result;
}

#include "mobile_loop.inc"

static void *listener_entry(void *argument)
{
	struct adapter_pvt *adapter = argument;
	struct timespec before;
	struct timespec after;
	assert(clock_gettime(CLOCK_THREAD_CPUTIME_ID, &before) == 0);
	void *result = do_sco_listen(adapter);
	assert(clock_gettime(CLOCK_THREAD_CPUTIME_ID, &after) == 0);
	assert(pthread_mutex_lock(&adapter->accept_io->lock) == 0);
	adapter->accept_io->thread_cpu = (after.tv_sec - before.tv_sec) +
		(after.tv_nsec - before.tv_nsec) / 1e9;
	adapter->accept_io->done = true;
	assert(pthread_cond_broadcast(&adapter->accept_io->changed) == 0);
	assert(pthread_mutex_unlock(&adapter->accept_io->lock) == 0);
	return result;
}

static void setup(struct io_context *ioc, struct sockaddr_un *address, socklen_t *length)
{
	pthread_condattr_t attributes;
	memset(ioc, 0, sizeof(*ioc));
	assert(pthread_mutex_init(&ioc->lock, NULL) == 0);
	assert(pthread_condattr_init(&attributes) == 0);
	assert(pthread_condattr_setclock(&attributes, CLOCK_MONOTONIC) == 0);
	assert(pthread_cond_init(&ioc->changed, &attributes) == 0);
	assert(pthread_condattr_destroy(&attributes) == 0);
	ioc->fd = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
	assert(ioc->fd >= 0);
	memset(address, 0, sizeof(*address));
	address->sun_family = AF_UNIX;
	int count = snprintf(address->sun_path + 1, sizeof(address->sun_path) - 1,
		"asterisk-mobile-harness-%ld-%u", (long) getpid(), ++test_number);
	assert(count > 0 && (size_t) count < sizeof(address->sun_path) - 1);
	*length = (socklen_t) (offsetof(struct sockaddr_un, sun_path) + 1 + count);
	assert(bind(ioc->fd, (struct sockaddr *) address, *length) == 0);
	assert(listen(ioc->fd, 32) == 0);
	atomic_store(&unloading, false);
	atomic_store(&signals_seen, 0);
	atomic_store(&error_logs, 0);
}

static void cleanup(struct io_context *ioc)
{
	assert(close(ioc->fd) == 0);
	assert(pthread_cond_destroy(&ioc->changed) == 0);
	assert(pthread_mutex_destroy(&ioc->lock) == 0);
}

static int connect_client(const struct sockaddr_un *address, socklen_t length)
{
	int fd = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
	assert(fd >= 0);
	assert(connect(fd, (const struct sockaddr *) address, length) == 0);
	return fd;
}

static void await_counts(struct io_context *ioc, unsigned int waits,
	unsigned int accepts, unsigned int interrupts)
{
	struct timespec end = deadline_after(0.5);
	assert(pthread_mutex_lock(&ioc->lock) == 0);
	while (ioc->waits < waits || ioc->accepts < accepts || ioc->interrupts < interrupts) {
		assert(pthread_cond_timedwait(&ioc->changed, &ioc->lock, &end) == 0);
	}
	assert(pthread_mutex_unlock(&ioc->lock) == 0);
}

static void join_bounded(pthread_t thread, double bound)
{
	struct timespec end = deadline_after(bound);
	assert(pthread_clockjoin_np(thread, NULL, CLOCK_MONOTONIC, &end) == 0);
}

static void stop_listener(pthread_t thread)
{
	atomic_store_explicit(&unloading, true, memory_order_release);
	assert(pthread_kill(thread, SIGURG) == 0);
	join_bounded(thread, 1.5);
}

static void report(const char *name, const struct io_context *ioc, double duration)
{
	printf("{\"test\":\"%s\",\"passed\":true,\"duration_seconds\":%.6f,"
		"\"wait_calls\":%u,\"accepted\":%u,\"eintr\":%u,"
		"\"error_logs\":%u,\"thread_cpu_seconds\":%.9f}\n",
		name, duration, ioc->waits, ioc->accepts, ioc->interrupts,
		atomic_load(&error_logs), ioc->thread_cpu);
}

static void connection_test(bool ready_before_start, unsigned int connections,
	bool interrupt_before_connect)
{
	struct io_context ioc;
	struct sockaddr_un address;
	socklen_t length;
	pthread_t thread;
	int clients[16];
	assert(connections <= sizeof(clients) / sizeof(clients[0]));
	setup(&ioc, &address, &length);
	struct adapter_pvt adapter = {.accept_io = &ioc, .id = "test-adapter"};
	if (ready_before_start) {
		for (unsigned int index = 0; index < connections; ++index) {
			clients[index] = connect_client(&address, length);
		}
	}
	double start = monotonic_seconds();
	assert(pthread_create(&thread, NULL, listener_entry, &adapter) == 0);
	if (!ready_before_start) {
		await_counts(&ioc, 1, 0, 0);
		pause_ms(20);
		if (interrupt_before_connect) {
			assert(pthread_kill(thread, SIGURG) == 0);
			await_counts(&ioc, 2, 0, 1);
		}
		start = monotonic_seconds();
		for (unsigned int index = 0; index < connections; ++index) {
			clients[index] = connect_client(&address, length);
		}
	}
	await_counts(&ioc, 1, connections, interrupt_before_connect ? 1 : 0);
	double latency = monotonic_seconds() - start;
	assert(latency < 0.3);
	stop_listener(thread);
	assert(atomic_load(&error_logs) == 0);
	report(interrupt_before_connect ? "eintr_then_connect" :
		ready_before_start ? "queued_before_wait" : "arrives_during_wait", &ioc, latency);
	for (unsigned int index = 0; index < connections; ++index) {
		assert(close(clients[index]) == 0);
	}
	cleanup(&ioc);
}

static void stop_before_start_test(void)
{
	struct io_context ioc;
	struct sockaddr_un address;
	socklen_t length;
	pthread_t thread;
	setup(&ioc, &address, &length);
	struct adapter_pvt adapter = {.accept_io = &ioc, .id = "test-adapter"};
	atomic_store(&unloading, true);
	double start = monotonic_seconds();
	assert(pthread_create(&thread, NULL, listener_entry, &adapter) == 0);
	join_bounded(thread, 0.5);
	assert(ioc.waits == 0);
	report("stop_before_start", &ioc, monotonic_seconds() - start);
	cleanup(&ioc);
}

static void idle_wait_test(void)
{
	struct io_context ioc;
	struct sockaddr_un address;
	socklen_t length;
	pthread_t thread;
	setup(&ioc, &address, &length);
	struct adapter_pvt adapter = {.accept_io = &ioc, .id = "test-adapter"};
	double start = monotonic_seconds();
	assert(pthread_create(&thread, NULL, listener_entry, &adapter) == 0);
	await_counts(&ioc, 1, 0, 0);
	pause_ms(1200);
	stop_listener(thread);
	assert(ioc.waits >= 1 && ioc.waits <= 3);
	assert(ioc.accepts == 0 && atomic_load(&error_logs) == 0);
	report("idle_has_no_millisecond_polling", &ioc, monotonic_seconds() - start);
	cleanup(&ioc);
}

static void lost_signal_test(void)
{
	struct io_context ioc;
	struct sockaddr_un address;
	socklen_t length;
	pthread_t thread;
	setup(&ioc, &address, &length);
	ioc.gate_first_wait = true;
	struct adapter_pvt adapter = {.accept_io = &ioc, .id = "test-adapter"};
	assert(pthread_create(&thread, NULL, listener_entry, &adapter) == 0);
	await_counts(&ioc, 1, 0, 0);
	/* Force stop and deliver SIGURG after check_unloading(), before poll(). */
	atomic_store_explicit(&unloading, true, memory_order_release);
	assert(pthread_kill(thread, SIGURG) == 0);
	double signal_deadline = monotonic_seconds() + 0.3;
	while (atomic_load_explicit(&signals_seen, memory_order_relaxed) == 0) {
		assert(monotonic_seconds() < signal_deadline);
		pause_ms(1);
	}
	double start = monotonic_seconds();
	assert(pthread_mutex_lock(&ioc.lock) == 0);
	ioc.gate_open = true;
	assert(pthread_cond_broadcast(&ioc.changed) == 0);
	assert(pthread_mutex_unlock(&ioc.lock) == 0);
	join_bounded(thread, 1.5);
	double elapsed = monotonic_seconds() - start;
	assert(elapsed >= 0.8 && elapsed < 1.5);
	assert(ioc.waits == 1 && ioc.interrupts == 0);
	assert(atomic_load(&error_logs) == 0);
	report("lost_sigurg_before_poll_is_bounded", &ioc, elapsed);
	cleanup(&ioc);
}

static void fatal_error_test(void)
{
	struct io_context ioc;
	struct sockaddr_un address;
	socklen_t length;
	pthread_t thread;
	setup(&ioc, &address, &length);
	ioc.inject_fatal_error = true;
	struct adapter_pvt adapter = {.accept_io = &ioc, .id = "test-adapter"};
	double start = monotonic_seconds();
	assert(pthread_create(&thread, NULL, listener_entry, &adapter) == 0);
	join_bounded(thread, 0.5);
	assert(ioc.waits == 1 && atomic_load(&error_logs) == 1);
	report("fatal_error_exits_without_busy_loop", &ioc, monotonic_seconds() - start);
	cleanup(&ioc);
}

int main(void)
{
	struct sigaction action = {.sa_handler = signal_handler};
	assert(atomic_is_lock_free(&signals_seen));
	assert(sigemptyset(&action.sa_mask) == 0);
	assert(sigaction(SIGURG, &action, NULL) == 0);
	stop_before_start_test();
	connection_test(true, 1, false);
	connection_test(false, 1, false);
	connection_test(true, 8, false);
	connection_test(false, 1, true);
	idle_wait_test();
	lost_signal_test();
	fatal_error_test();
	return 0;
}
