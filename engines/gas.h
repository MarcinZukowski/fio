/**
 * GAS - Generic ASync framework.
 * Can be used to add (async) testing to fio for any kind of request
 * (e.g. disk, network, web etc).
 */

#include <pthread.h>

#include "../fio.h"

// From https://github.com/Pithikos/C-Thread-Pool
#include "thpool.h"

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

	void (*worker)(void *);
};

/** A single in-flight GAS request */
struct gas_io {
	struct io_u *io_u;
	struct gas_data *gas_data;
	void *backend_data;
};

void gas_init_async(struct thread_data *td, void (*perform_work)(void *));

void gas_register_async(struct ioengine_ops *ops);
