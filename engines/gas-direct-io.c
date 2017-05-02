/**
 * gas-direct-io
 *
 * An example back-end for GAS
 */

#include <assert.h>
#include <malloc.h>
#include <unistd.h>

#include "../fio.h"
#include "../optgroup.h"

#include "gas.h"

static struct fio_option gas_direct_io_options[] = {
    {
        .name	= NULL,
    },
};

// Alignment
#define BLKSIZE 16*1024

// Single IO size
#define BUFSIZE 256*1024

static __thread char *buf = NULL;

static void direct_read(const char *fname, unsigned long long offset, unsigned long long size)
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

  direct_read(io_u->file->file_name, io_u->offset, io_u->xfer_buflen);
}

static int gas_direct_io_open_file(struct thread_data *td, struct fio_file *f)
{
  // Nothing to do
  return 0;
}

static int gas_direct_io_close_file(struct thread_data *td, struct fio_file *f)
{
  // Nothing to do
  return 0;
}

static int gas_direct_io_init_async(struct thread_data *td)
{
  gas_init_async(td, perform_work);
  return 0;
}

static struct ioengine_ops gas_direct_io_ioengine = {
    .name			= "gas-direct-io",
    .version		= FIO_IOOPS_VERSION,
    .init			= gas_direct_io_init_async,

    .open_file		= gas_direct_io_open_file,
    .close_file		= gas_direct_io_close_file,
    .option_struct_size	= sizeof(gas_direct_io_options),
    .options		= gas_direct_io_options,
  	.flags			= FIO_DISKLESSIO | FIO_NODISKUTIL | FIO_FAKEIO
};

static void fio_init fio_gas_direct_io_register(void)
{
  gas_register_async(&gas_direct_io_ioengine);
}
