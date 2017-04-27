#include "../fio.h"
#include "../optgroup.h"

#include "gas.h"

#include "s3_worker.h"

static int s3_common_init()
{
  s3_init();
  return 0;
}

/**
 * Options state for GAS
 */
struct s3_options {
  void *pad;
  unsigned int timeout;
};

/**
 * S3 options definitions
 */
static struct fio_option options[] = {
    {
      .name	= "s3_timeout",
      .lname	= "S3 request timeout",
      .type	= FIO_OPT_INT,
      .off1	= offsetof(struct s3_options, timeout),
      .help	= "How long we wait before canceling an S3 request",
      .category = FIO_OPT_C_ENGINE,
      .group	= FIO_OPT_G_LIBAIO,
    },
    {
        .name	= NULL,
    },
};

static void perform_work(void *arg)
{
  struct gas_io *gas_io = (struct gas_io *) arg;

  struct io_u *io_u = gas_io->io_u;

  s3_read(&gas_io->backend_data, io_u->file->file_name, io_u->offset, io_u->xfer_buflen);
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
  return 0;
}

static struct ioengine_ops s3_ioengine_async = {
    .name			= "s3",
    .version		= FIO_IOOPS_VERSION,
    .init			= s3_init_async,

    .open_file		= s3_open_file,
    .close_file		= s3_close_file,
//    .get_file_size		= generic_get_file_size,
    .option_struct_size	= sizeof(struct s3_options),
    .options		= options,
  	.flags			= FIO_DISKLESSIO | FIO_NODISKUTIL | FIO_FAKEIO
};

static void fio_init fio_gas_register(void)
{
  gas_register_async(&s3_ioengine_async);
}
