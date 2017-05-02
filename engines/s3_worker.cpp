#include "s3_worker.h"

#include <unistd.h>
#include <string.h>

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/core/client/ClientConfiguration.h>
#include <fstream>

thread_local Aws::S3::S3Client *s3_client = nullptr;

extern "C"
{

void s3_init()
{
  Aws::SDKOptions options;
  Aws::InitAPI(options);
}

int s3_read(struct s3_config *s3_config, void **backend_data, const char *fname, size_t offset, size_t size)
{
  size_t len = strlen(fname) + 1;
  char *str = alloca(len);
  memcpy(str, fname, len);

  char *slash = strchr(str, '/');
  assert(slash);
  *slash = '\0';

  const Aws::String bucket_name = str ;
  const Aws::String key_name = slash + 1;

  if (s3_client == nullptr)
  {
    if (s3_config->verbose) {
      printf("Creating new S3Client\n");
    }
    fflush(stdout);

    Aws::Client::ClientConfiguration clientConfig;
    if (s3_config->region) {
      clientConfig.region = s3_config->region;
    }
    s3_client = new Aws::S3::S3Client(clientConfig);
  }

  char *range = alloca(1000);
  sprintf(range, "bytes=%zd-%zd", offset, offset + size - 1);

  Aws::S3::Model::GetObjectRequest object_request;
  object_request.WithBucket(bucket_name).WithKey(key_name);
  object_request.SetRange(range);

  if (s3_config->verbose) {
    printf("[%p] Issuing request: %s / %s %zd %zd\n", s3_client, bucket_name.c_str(), key_name.c_str(), offset, size);
  }

  auto get_object_outcome = s3_client->GetObject(object_request);

  if (s3_config->verbose) {
    printf("[%p] Done request\n", s3_client);
  }

  if (get_object_outcome.IsSuccess()) {
    return 0;
  } else {
    std::cout << "GetObject error: " <<
              get_object_outcome.GetError().GetExceptionName() << " " <<
              get_object_outcome.GetError().GetMessage() << std::endl;
    return 1;
  }
}

}  // extern C
