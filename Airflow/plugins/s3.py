# Check Bucket Connection
def check_buckets_connection(bucket_list, s3_hook):
    for bucket_name in bucket_list:
        s3_object = s3_hook.list_keys(bucket_name=bucket_name)
        print(f"Connected to the {bucket_name} in MinIO successfully...", s3_object)

# Ingest Document from Bucket    
def ingest_document(file_key: str, bucketName: str, localPath: str, s3_hook):
    try:
        s3_object = s3_hook.get_key(key=file_key, bucket_name=bucketName)
        if s3_object:
            with open(localPath, "wb") as f:
                s3_object.download_fileobj(f)
            print(f"Downloaded {file_key} into local environment at {localPath}")
        else:
            print(f"File '{file_key}' not found in bucket '{bucketName}'.")
    except Exception as e:
        print(f"Error downloading file '{file_key}': {e}")

# Upload Document to Bucket 
def upload_file_to_bucket(fileKey: str, bucketName: str, uploadFile: str, s3_hook):
    s3_hook.load_file(
        filename=uploadFile,
        key=fileKey,
        bucket_name=bucketName,
        replace=True
    )
    print(f"Baseline file {uploadFile} uploaded successfully to {bucketName}/{fileKey}")