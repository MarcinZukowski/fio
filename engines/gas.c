/**
 * GAS - Generic ASync framework.
 * Can be used to add (async) testing to fio for any kind of request
 * (e.g. disk, network, web etc).
 */
#include "gas.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <assert.h>
#include <libaio.h>

#include "../lib/pow2.h"
#include "../optgroup.h"
#include "../io_ddir.h"

/**
 * Allocate a new QOP
 * @return
 */
qop* qop_new(int capacity)
{
	qop *q;

	assert(capacity > 0);
	q = calloc(1, sizeof(qop));
	q->capacity = capacity;
	q->pointers = calloc(capacity, sizeof(*q->pointers));
	q->used = 0;
	q->head = 0;

	return q;
}

void qop_push(qop *q, void *ptr)
{
	assert(q->used < q->capacity);
	q->pointers[q->head] = ptr;
	q->head = (q->head + 1) % q->capacity;
	q->used++;
}

void* qop_pop(qop *q)
{
	void *res;
	assert(q->used > 0);

	res = q->pointers[(q->head + q->capacity - q->used) % q->capacity];
	q->used--;

	return res;
}

int qop_available(qop *q)
{
	return q->capacity - q->used;
}

int qop_used(qop *q)
{
	return q->used;
}

/**
 * Options state for GAS
 */
struct gas_options {
	void *pad;
};

/**
 * GAS options definitions
 */
static struct fio_option options[] = {
	{
		.name = NULL,
	},
};

/**
 * Initializes GAS state
 */
static int fio_gas_init(struct thread_data *td)
{

	struct gas_options *o = td->eo;
	struct gas_data *d = td->io_ops_data;
	int res;

	assert(d == NULL);
	d = calloc(1, sizeof(*d));

	d->depth = td->o.iodepth;

	d->queued_io_us = qop_new(d->depth);
	d->done_gas_ios = qop_new(d->depth);

	d->last_done_gas_ios = calloc(d->depth, sizeof(struct gas_io *));
	d->last_done_used = 0;

	res = pthread_mutex_init(&d->done_mutex, NULL);
	assert(res == 0);

	d->thpool = thpool_init(d->depth);

	td->io_ops_data = d;

	return 0;
}

/**
 * Prepares a single io_u - just initialize the gas_io struct
 */
static int fio_gas_prep(struct thread_data *td, struct io_u *io_u)
{
	struct gas_io *io = io_u->gas_io;
	struct gas_data *d = td->io_ops_data;

	if (!io) {
		io = malloc(sizeof(*io));
		io_u->gas_io = io;
	}

	memset(io, 0, sizeof(*io));
	io->io_u = io_u;
	io->gas_data = d;

	if (io_u->ddir != DDIR_READ) {
		log_err("gas: ddir not supported: %d\n", io_u->ddir);
	}

	return 0;
}

/**
 * Queues a single io for execution
 */
static int fio_gas_queue(struct thread_data *td, struct io_u *io_u)
{
	struct gas_data *d = td->io_ops_data;
	struct gas_io *gas_io;

	fio_ro_check(td, io_u);

	if (qop_available(d->queued_io_us) == 0) {
		return FIO_Q_BUSY;
	}

	if (io_u->ddir != DDIR_READ) {
		log_err("gas: ddir not supported: %d\n", io_u->ddir);
	}

	if (ddir_sync(io_u->ddir)) {
		log_err("gas: ddir_sync not supported: %d\n", io_u->ddir);
	}

	gas_io = io_u->gas_io;

	assert(gas_io != NULL);
	assert(gas_io->io_u == io_u);

	qop_push(d->queued_io_us, io_u);
	return FIO_Q_QUEUED;
}

/**
 * Mark a single event as queued (issued)
 * @param io_us  Array of io_u to update
 * @param nr     Size of io_us
 */
static void fio_gas_queued(struct thread_data *td, struct io_u **io_us,
			   unsigned int nr)
{
	struct timeval now;
	unsigned int i;

	if (!fio_fill_issue_time(td))
		return;

	fio_gettime(&now, NULL);

	for (i = 0; i < nr; i++) {
		struct io_u *io_u = io_us[i];

		memcpy(&io_u->issue_time, &now, sizeof(now));
		io_u_queued(td, io_u);
	}
}


static void worker_wrapper(void *arg)
{
	struct gas_io *gas_io = (struct gas_io *) arg;
	struct gas_data *d = gas_io->gas_data;

	int du;

	// Call the actual worker
	d->worker(arg);

	pthread_mutex_lock(&d->done_mutex);
	qop_push(d->done_gas_ios, gas_io);
	du = d->done_gas_ios->used;
	pthread_mutex_unlock(&d->done_mutex);
}

static void sleepy_worker(void *arg)
{
	usleep(100);
}

/**
 * @return 0 if success
 */
static int fio_gas_commit(struct thread_data *td)
{
	struct gas_data *d = td->io_ops_data;
	int ret = 0;

	while (qop_used(d->queued_io_us)) {
		struct io_u *io_u = qop_pop(d->queued_io_us);

		thpool_add_work(d->thpool, worker_wrapper, io_u->gas_io);
		// perform_work(io_u->gas_io);

		fio_gas_queued(td, &io_u, 1);
		io_u_mark_submit(td, 1);
	}

	return ret;
}


static int fio_gas_getevents(struct thread_data *td, unsigned int min,
			     unsigned int max, const struct timespec *t)
{
	struct gas_data *d = td->io_ops_data;
	int events = 0;

	assert(0 <= min);
	assert(min <= max && 0 < max);

	do {
		int take;

		pthread_mutex_lock(&d->done_mutex);
		take = d->done_gas_ios->used;
		if (events + take > max)
			take = max - events;
		while (take) {
			d->last_done_gas_ios[events++] = qop_pop(d->done_gas_ios);
			take--;
		}
		assert(events <= max);
		d->last_done_used = events;
		pthread_mutex_unlock(&d->done_mutex);
		if (events < min) {
			usleep(10);
		}
	} while (events < min);

	return events;
}


static struct io_u *fio_gas_event(struct thread_data *td, int event)
{
	struct gas_data *d = td->io_ops_data;
	struct io_u *io_u;

	assert(event < d->last_done_used);

	io_u = d->last_done_gas_ios[event]->io_u;

	return io_u;
}

void gas_init_async(struct thread_data *td, void (*worker)(void *))
{
	struct gas_data *d;

	fio_gas_init(td);

	d = td->io_ops_data;
	d->worker = worker;
}

static int gas_cancel(struct thread_data *td, struct io_u *io_u)
{
	struct gas_data *ld = td->io_ops_data;

	printf("Received cancel\n");
	fflush(stdout);

	return -1;
}

void gas_register_async(struct ioengine_ops *ops)
{
	assert(ops->init);

	ops->prep = fio_gas_prep;
	ops->queue = fio_gas_queue;
	ops->commit = fio_gas_commit;
	ops->getevents = fio_gas_getevents;
	ops->event = fio_gas_event;
	ops->cancel = gas_cancel;

	register_ioengine(ops);
}


static int gas_init(struct thread_data *td)
{
	gas_init_async(td, sleepy_worker);
	return 0;
}

static struct ioengine_ops gas_ioengine = {
	.name               = "gas",
	.version            = FIO_IOOPS_VERSION,

	.init               = gas_init,

	.open_file          = generic_open_file,
	.close_file         = generic_close_file,
	.get_file_size      = generic_get_file_size,
	.cancel             = gas_cancel,
	.option_struct_size = sizeof(struct gas_options),
	.options            = options,
};

static void fio_init fio_gas_register(void)
{
	gas_register_async(&gas_ioengine);
}

static void fio_exit fio_gas_unregister(void)
{
	unregister_ioengine(&gas_ioengine);
}
