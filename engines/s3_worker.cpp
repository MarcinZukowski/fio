#include "s3_worker.h"

#include <unistd.h>

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <fstream>


extern "C" {

void s3_init()
{
  Aws::SDKOptions options;
  Aws::InitAPI(options);
}

int s3_read(const char *fname, size_t offset, size_t size)
{
  printf("Reading %s\n", fname);
  usleep(500000);
}

}
