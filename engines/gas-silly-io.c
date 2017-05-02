#include <assert.h>
#include <malloc.h>
#include <unistd.h>

#include "../fio.h"
#include "../optgroup.h"

#include "gas.h"

static struct fio_option gas_silly_io_options[] = {
    {
        .name	= NULL,
    },
};

#define BLKSIZE 16*1024

#define BUFSIZE 256*1024

static __thread char *buf = NULL;

static void silly_read(const char *fname, unsigned long long offset, unsigned long long size)
{
  ssize_t res;
  int fd = open(fname, O_RDONLY | O_DIRECT);
  assert(fd > 0);

  if (!buf)
  {
    buf = memalign(BLKSIZE, BUFSIZE);
  }

  // Round down
  offset = offset & ~(BLKSIZE);

  res = lseek(fd, offset, SEEK_SET);
  assert(res == offset);

  while (size > 0)
  {
    ssize_t toRead = (size > BUFSIZE) ? BUFSIZE : size;
    res = read(fd, buf, toRead);
    if (res != toRead)
    {
      printf("ERROR: %zd vs %zd, at %llx\n", res, toRead, offset);
      perror("");
      assert (res == toRead);
    }

    size -= toRead;
  }

  close(fd);
}

static void perform_work(void *arg)
{
  struct gas_io *gas_io = (struct gas_io *) arg;

  struct io_u *io_u = gas_io->io_u;

  silly_read(io_u->file->file_name, io_u->offset, io_u->xfer_buflen);
}

static int gas_silly_io_open_file(struct thread_data *td, struct fio_file *f)
{
  // Nothing to do
  return 0;
}

static int gas_silly_io_close_file(struct thread_data *td, struct fio_file *f)
{
  // Nothing to do
  return 0;
}

static int gas_silly_io_init_async(struct thread_data *td)
{
  gas_init_async(td, perform_work);
  return 0;
}

static struct ioengine_ops gas_silly_io_ioengine = {
    .name			= "gas-silly-io",
    .version		= FIO_IOOPS_VERSION,
    .init			= gas_silly_io_init_async,

    .open_file		= gas_silly_io_open_file,
    .close_file		= gas_silly_io_close_file,
//    .get_file_size		= generic_get_file_size,
    .option_struct_size	= sizeof(gas_silly_io_options),
    .options		= gas_silly_io_options,
  	.flags			= FIO_DISKLESSIO | FIO_NODISKUTIL | FIO_FAKEIO
};

static void fio_init fio_gas_silly_io_register(void)
{
  gas_register_async(&gas_silly_io_ioengine);
}
