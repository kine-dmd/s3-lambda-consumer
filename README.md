# s3-lambda-consumer

The S3 lambda consumer is designed to be triggered when a binary file is written to the S3 intermediary bucket. It downloads the file straight to memory and then transforms the data to Apache Parquet format before writing it to the final S3 bucket in the same file path.
