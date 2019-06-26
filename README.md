# S3 Lambda Consumer for Apple Watch 3 [![Build Status](https://travis-ci.org/kine-dmd/s3-lambda-consumer.svg?branch=master)](https://travis-ci.org/kine-dmd/s3-lambda-consumer)

The S3 lambda consumer is designed to be triggered when a binary file is written to the S3 intermediary bucket. It downloads the file straight to memory and then transforms the data to Apache Parquet format before writing it to the final S3 bucket in the same file path. This is illustrated below:
![s3Lambda](https://user-images.githubusercontent.com/26333869/60193618-e4249b80-982f-11e9-8cf5-1df49b7adfb6.png)


This lambda function must be compiled and deployed to AWS lambda with the configuration shown below. This is done automatically through Travis CI when commits are merged to master.
![lambdaConsole](https://user-images.githubusercontent.com/26333869/60192869-8fccec00-982e-11e9-995b-47771acadd21.png)

