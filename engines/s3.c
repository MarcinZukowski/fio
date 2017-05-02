#include "../fio.h"
#include "../optgroup.h"

#include "gas.h"

#include "s3_worker.h"

static int s3_common_init()
{
  s3_init();
  return 0;
}

static struct s3_config s3_config = { NULL };

/**
 * S3 options definitions
 */
static int str_region_cb(void *data, const char *input)
{
  if (s3_config.region)
    free(s3_config.region);
  s3_config.region = strdup(input);
  return 0;
}

struct fio_option options[] = {
    {
      .name	= "s3_region",
      .lname	= "AWS Region, e.g. 'us-west-2'",
      .type	= FIO_OPT_STR_STORE,
      .cb	= str_region_cb,
      .help	= "Which region to use",
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

  s3_read(&s3_config, &gas_io->backend_data, io_u->file->file_name, io_u->offset, io_u->xfer_buflen);
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
    .option_struct_size	= sizeof(options),
    .options		= options,
  	.flags			= FIO_DISKLESSIO | FIO_NODISKUTIL | FIO_FAKEIO
};

static void fio_init fio_gas_register(void)
{
  gas_register_async(&s3_ioengine_async);
}
