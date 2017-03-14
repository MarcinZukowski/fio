/**
 * GAS - Generic ASync framework.
 * Can be used to add (async) testing to fio for any kind of request
 * (e.g. disk, network, web etc).
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <assert.h>
#include <libaio.h>

#include "../fio.h"
#include "../lib/pow2.h"
#include "../optgroup.h"
#include "../io_ddir.h"

// From https://github.com/Pithikos/C-Thread-Pool
#include "thpool.h"

// #define pr(x...) printf(x)
#define pr(x...)

/**
 * Queue of pointers
 */
typedef struct {
  int capacity;
  void **pointers;
  int used;
  int head;
} qop;

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
 * The global state of GAS
 */
struct gas_data {
	int depth;

  /** io_us ready to be committed */
  qop *queued_io_us;

	/** Entries that are finished */
  qop *done_gas_ios;

  struct gas_io **last_done_gas_ios;
  unsigned int last_done_used;

	pthread_mutex_t done_mutex;

	threadpool thpool;
};

/** A single in-flight GAS request */
struct gas_io {
	struct io_u* io_u;
	struct gas_data *gas_data;
};

/**
 * Options state for GAS
 */
struct gas_options {
	void *pad;
	unsigned int timeout;
};

/**
 * GAS options definitions
 */
static struct fio_option options[] = {
	{
		.name	= "timeout",
		.lname	= "S3 request timeout",
		.type	= FIO_OPT_INT,
		.off1	= offsetof(struct gas_options, timeout),
		.help	= "How long we wait before canceling an S3 request",
		.category = FIO_OPT_C_ENGINE,
		.group	= FIO_OPT_G_LIBAIO,
	},
	{
		.name	= NULL,
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

	pr("fio_gas_init: timeout=%d\n", o->timeout);

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
	struct gas_io* io = io_u->gas_io;
	struct gas_data *d = td->io_ops_data;
  pr("fio_gas_prep: io_u  = %p\n", io_u);

	if (!io) {
		io = malloc(sizeof(*io));
		io_u->gas_io = io;
	}

	memset(io, 0, sizeof(*io));
	io->io_u = io_u;
	io->gas_data = d;

	if (io_u->ddir != DDIR_READ)
	{
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

  pr("fio_gas_queue: io_u  = %p\n", io_u);

	fio_ro_check(td, io_u);

  if (qop_available(d->queued_io_us) == 0)
		return FIO_Q_BUSY;

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


static void perform_work(void *arg)
{
	struct gas_io *gas_io = (struct gas_io *) arg;
	struct gas_data *d = gas_io->gas_data;

  int du;

  pr("perform_work: started on  io_u = %p\n", gas_io->io_u);

	// First, sleep a bit
	usleep(123);

	pthread_mutex_lock(&d->done_mutex);
  qop_push(d->done_gas_ios, gas_io);
  du = d->done_gas_ios->used;
	pthread_mutex_unlock(&d->done_mutex);

  pr("perform_work: finished on io_u = %p done_used=%d\n", gas_io->io_u, du);

}

/**
 * @return 0 if success
 */
static int fio_gas_commit(struct thread_data *td)
{
	struct gas_data *d = td->io_ops_data;
	int ret = 0;

  pr("fio_gas_commit\n");

  while (qop_used(d->queued_io_us)) {
		struct io_u *io_u = qop_pop(d->queued_io_us);

		thpool_add_work(d->thpool, perform_work, io_u->gas_io);
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

  pr("fio_gas_getevents: min=%d max=%d\n", min, max);

	assert(0 < min);
	assert(min <= max);

	do {
    int take;

		pthread_mutex_lock(&d->done_mutex);
    pr("fio_gas_getevents: done_used=%d\n", d->done_gas_ios->used);
    take = d->done_gas_ios->used;
    if (events + take > max)
      take = max - events;
    while (take)
    {
      d->last_done_gas_ios[events++] = qop_pop(d->done_gas_ios);
      take--;
    }
		assert(events <= max);
    d->last_done_used = events;
		pthread_mutex_unlock(&d->done_mutex);
		if (events < min)
		{
      pr("fio_gas_getevents: sleeping\n");
			usleep(10);
		}
	} while (events < min);

  pr("fio_gas_getevents: return %d\n", events);
  return events;
}



static struct io_u *fio_gas_event(struct thread_data *td, int event)
{
	struct gas_data *d = td->io_ops_data;

  assert (event < d->last_done_used);

	struct io_u *io_u = d->last_done_gas_ios[event]->io_u;
	io_u->error = 0;
  pr("fio_gas_getevent: event=%d last_done_used=%d returning %p\n", event, d->last_done_used, io_u);

	return io_u;
}

static struct ioengine_ops gas_ioengine = {
	.name			= "gas",
	.version		= FIO_IOOPS_VERSION,
 	.init			= fio_gas_init,
	.prep			= fio_gas_prep,
	.queue			= fio_gas_queue,
	.commit			= fio_gas_commit,
	.getevents		= fio_gas_getevents,
	.event			= fio_gas_event,
/*
	.cancel			= fio_libaio_cancel,
	.cleanup		= fio_libaio_cleanup,
*/
	.open_file		= generic_open_file,
	.close_file		= generic_close_file,
	.get_file_size		= generic_get_file_size,
  .option_struct_size	= sizeof(struct gas_options),
/*
 */
	.options		= options,
//	.flags			= FIO_DISKLESSIO
};

static void fio_init fio_gas_register(void)
{
	register_ioengine(&gas_ioengine);
}

static void fio_exit fio_gas_unregister(void)
{
	unregister_ioengine(&gas_ioengine);
}
