#include "../fio.h"
#include "../optgroup.h"

#include "gas.h"

#include "s3_worker.h"

static int s3_common_init()
{
	s3_init();
	return 0;
}

static struct s3_config global_s3_config;

/**
 * S3 options definitions
 */
struct fio_option s3_options[] = {
	{
		.name	= "s3_region",
		.lname	= "AWS Region, e.g. 'us-west-2'",
		.type	= FIO_OPT_STR_STORE,
		.off1   = offsetof(struct s3_config, region),
		.help	= "Which region to use",
		.category = FIO_OPT_C_ENGINE,
		.group	= FIO_OPT_G_GAS,
	},
	{
		.name	= "s3_verbose",
		.lname	= "S3 verbose logging",
		.type	= FIO_OPT_INT,
		.def    = 0,
		.off1	= offsetof(struct s3_config, verbose),
		.help	= "Enables extra logging for S3 access",
		.category = FIO_OPT_C_ENGINE,
		.group	= FIO_OPT_G_GAS,
	},
	{
		.name	= NULL,
	},
};

static void perform_work(void *arg)
{
	struct gas_io *gas_io = (struct gas_io *) arg;

	struct io_u *io_u = gas_io->io_u;

	int res = s3_read(&global_s3_config, &gas_io->backend_data, io_u->file->file_name, io_u->offset,
			  io_u->xfer_buflen);

	io_u->error = -res;
}

static int s3_open_file(struct thread_data *td, struct fio_file *f)
{
	// Nothing to do
	return 0;
}

static int s3_close_file(struct thread_data *td, struct fio_file *f)
{
	// Nothing to do
	return 0;
}

static int s3_init_async(struct thread_data *td)
{
	s3_common_init();
	gas_init_async(td, perform_work);

	// Remember our config
	global_s3_config = *(struct s3_config*) td->eo;

	return 0;
}

static struct ioengine_ops s3_ioengine_async = {
	.name               = "s3",
	.version            = FIO_IOOPS_VERSION,
	.init               = s3_init_async,

	.open_file          = s3_open_file,
	.close_file         = s3_close_file,
	.option_struct_size = sizeof(struct s3_config),
	.options            = s3_options,
	.flags              = FIO_DISKLESSIO | FIO_NODISKUTIL | FIO_FAKEIO
};

static void fio_init fio_gas_register(void)
{
	gas_register_async(&s3_ioengine_async);
}
