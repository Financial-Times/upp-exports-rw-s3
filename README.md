# UPP Exports Reader/Writer for S3 

[![Circle CI](https://circleci.com/gh/Financial-Times/upp-exports-rw-s3.svg?style=shield)](https://circleci.com/gh/Financial-Times/upp-exports-rw-s3) [![Coverage Status](https://coveralls.io/repos/github/Financial-Times/upp-exports-rw-s3/badge.svg)](https://coveralls.io/github/Financial-Times/upp-exports-rw-s3)
 
## system-code: upp-content-s3-rw
## Introduction
An API for reading/writing content and concepts payloads up to S3.

## Installation

```shell script
go get github.com/Financial-Times/upp-exports-rw-s3
cd $GOPATH/src/github.com/Financial-Times/upp-exports-rw-s3
go build -mod=readonly 
```

## Running locally

```shell script
export|set PORT=8080
export|set BUCKET_NAME='bucketName'
export|set AWS_REGION="eu-west-1"
$GOPATH/bin/upp-exports-rw-s3
```
The app assumes that you have correctly set up your AWS credentials by either using the `~/.aws/credentials` file:

```
[default]
aws_access_key_id = AKID1234567890
aws_secret_access_key = MY-SECRET-KEY
```

or the default AWS environment variables:

```
AWS_ACCESS_KEY_ID=AKID1234567890
AWS_SECRET_ACCESS_KEY=MY-SECRET-KEY
```

There are optional arguments as well:
```
export|set BUCKET_PREFIX="bucketPrefix" # adds a prefix folder to all items uploaded
export|set WORKERS=10 # Number of concurrent downloads when downloading all items. Default is 10
export|set SRC_CONCURRENT_PROCESSING=true # Whether the consumer uses concurrent processing for the messages
export|set BUCKET_CONCEPT_PREFIX= #name of the s3 folder where concepts should be stored
export|set BUCKET_CONTENT_PREFIX= #name of the s3 folder where content should be stored
export|set CONTENT_RESOURCE_PATH= #url prefix for endpoint that performs rw operations on content
export|set CONCEPT_RESOURCE_PATH= #url prefix for endpoint that performs rw operations on content
```

### Run locally with specified resource path
`$GOPATH/bin/upp-exports-rw-s3 --port=8080 --resourcePath="concepts" --bucketName="bucketName" --bucketContentPrefix="bucketPrefix" --bucketConceptPrefix="bucketPrefix" --awsRegion="eu-west-1"`

## Test locally
See Endpoints section.

## Build and deployment
* Docker Hub builds: [coco/upp-exports-rw-s3](https://hub.docker.com/r/coco/upp-exports-rw-s3/)
* Cluster deployment:  [upp-exports-rw-s3@.service](https://github.com/Financial-Times/pub-service-files), [upp-exports-rw-s3@service](https://github.com/Financial-Times/up-service-files)
* CI provided by CircleCI: [upp-exports-rw-s3](https://circleci.com/gh/Financial-Times/upp-exports-rw-s3)
* Code coverage provided by Coverall: [upp-exports-rw-s3](https://coveralls.io/github/Financial-Times/upp-exports-rw-s3)

## Service Endpoints
For complete API specification see [S3 Read/Write API Endpoint](https://docs.google.com/document/d/1Ck-o0Le9cXOfm-aVjiGmOT7ZTB5W5fDTsPqGkhzfa-U/edit#)

### Content PUT <CONTENT_RESOURCE_PATH>/UUID?date=<DATE>

Any payload can be written via the PUT using a unique UUID to identify this payload within the S3 bucket

```
curl -H 'Content-Type: application/json' -X PUT -d '{"tags":["tag1","tag2"],"question":"Which band?","answers":[{"id":"a0","answer":"Answer1"},{"id":"a1","answer":"answer2"}]}' http://localhost:8080/bcac6326-dd23-4b6a-9dfa-c2fbeb9737d9
```

The `Content-Type` is important as that will be what the file will be stored as.
In addition we will also store transaction ID in S3. It is either provided as request header and if not, it is auto-generated.

When the content is uploaded, the key generated for the item is converted from 
`123e4567-e89b-12d3-a456-426655440000` to `<bucket_prefix>/123e4567/e89b/12d3/a456/426655440000`. 
The reason we do this is so that it becomes easier to manage/browser for content in the AWS console. 
It is also good practice to do this as it means that files get put into different partitions. 
This is important if you're writing and pulling content from S3 as it means that content will get written/read from different partitions on S3.

### Concept PUT <CONCEPT_RESOURCE_PATH>/FILE_NAME
Will upload the file with FILE_NAME and file content provided as request payload to S3.

### Content GET <CONTENT_RESOURCE_PATH>/UUID?date=<DATE>
This internal read should return what was written to S3

If not found, you'll get a 404 response.

```
curl http://localhost:8080/bcac6326-dd23-4b6a-9dfa-c2fbeb9737d9
```

### Concept GET <CONCEPT_RESOURCE_PATH>/FILE_NAME
This internal read should return the file with FILE_NAME from s3 concept folder.

### Content DELETE <CONTENT_RESOURCE_PATH>/UUID
Will return 204 if delete was successful, or 404 if the file with UUID was not found.

### Concept DELETE <CONCEPT_RESOURCE_PATH>/FILE_NAME
Will return 204 if delete was successful, or 404 if the file with FILE_NAME was not found.

### Admin endpoints

Healthchecks: [http://localhost:8080/__health](http://localhost:8080/__health)  
Build Info: [http://localhost:8080/__build-info](http://localhost:8080/build-info) or [http://localhost:8080/build-info](http://localhost:8080/__build-info)   
GTG: [http://localhost:8080/__gtg](http://localhost:8080/__gtg) 


### Other Information

#### S3 buckets

For this to work you need to make sure that your AWS credentials has the following policy file on the bucket.
```
{
	"Version": "2012-10-17",
	"Id": "Policy12345678990",
	"Statement": [
		{
			"Sid": "Stmt12345678990",
			"Effect": "Allow",
			"Principal": {
				"AWS": "arn:aws:iam::<ID>:user/<AWS_KEY_ID>"
			},
			"Action": "s3:*",
			"Resource": [
				"arn:aws:s3:::<BUCKET_NAME>",
				"arn:aws:s3:::<BUCKET_NAME>/*"
			]
		}
	]
}
```
