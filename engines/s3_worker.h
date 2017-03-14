#ifndef _S3_WORKER_H_
#define _S3_WORKER_H_

#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

void s3_init();
int s3_read(const char *fname, size_t offset, size_t size);


#ifdef __cplusplus
}
#endif

#endif  // _S3_WORKER_H_