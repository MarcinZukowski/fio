#include "s3_worker.h"

#include <unistd.h>
#include <string.h>

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/core/client/ClientConfiguration.h>
#include <fstream>

extern "C"
{

void s3_init()
{
  Aws::SDKOptions options;
  Aws::InitAPI(options);
}

int s3_read(const char *fname, size_t offset, size_t size)
{
  size_t len = strlen(fname) + 1;
  char *str = alloca(len);
  memcpy(str, fname, len);

  char *slash = strchr(str, '/');
  assert(slash);
  *slash = '\0';

  const Aws::String bucket_name = str ;
  const Aws::String key_name = slash + 1;

  printf("Reading '%s'  /  '%s'   %lld:%lld\n", bucket_name.c_str(), key_name.c_str(), offset, size);

//  Aws::Client::ClientConfiguration config;
//  config.region = "us-west-2";
//  config.endpointOverride= "http://s3-us-west-2.amazonaws.com";

  Aws::S3::S3Client s3_client;

  char *range = alloca(1000);
  sprintf(range, "bytes=%lld-%lld", offset, offset + size - 1);

  Aws::S3::Model::GetObjectRequest object_request;
  object_request.WithBucket(bucket_name).WithKey(key_name);
  object_request.SetRange(range);

  auto get_object_outcome = s3_client.GetObject(object_request);

  if (get_object_outcome.IsSuccess()) {
//    Aws::OFStream local_file;
//    local_file.open(key_name.c_str(), std::ios::out | std::ios::binary);
//    local_file << get_object_outcome.GetResult().GetBody().rdbuf();
    std::cout << "Done!" << std::endl;
  } else {
    std::cout << "GetObject error: " <<
              get_object_outcome.GetError().GetExceptionName() << " " <<
              get_object_outcome.GetError().GetMessage() << std::endl;
  }
}

}  // extern C
