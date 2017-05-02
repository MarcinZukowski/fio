#ifndef _S3_WORKER_H_
#define _S3_WORKER_H_

#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

struct s3_config
{
  char *region;
};

void s3_init();
int s3_read(struct s3_config *config, void **backend_data, const char *fname, size_t offset, size_t size);


#ifdef __cplusplus
}
#endif

#endif  // _S3_WORKER_H_