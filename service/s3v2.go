package service

import (
	"bytes"
	"context"

	transactionid "github.com/Financial-Times/transactionid-utils-go"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	log "github.com/sirupsen/logrus"
)

type S3Client2 struct {
	client     *s3.Client
	bucketName string
}

func NewS3Client2(client *s3.Client, bucketName string) *S3Client2 {
	return &S3Client2{client, bucketName}
}

func (c *S3Client2) Write(s3ObjectKey string, b *[]byte, ct string, tid string) error {
	s3Param := &s3.PutObjectInput{
		Bucket: aws.String(c.bucketName),
		Key:    aws.String(s3ObjectKey),
		Body:   bytes.NewReader(*b),
	}
	if ct != "" {
		s3Param.ContentType = aws.String(ct)
	}

	if s3Param.Metadata == nil {
		s3Param.Metadata = make(map[string]string)
	}
	s3Param.Metadata[transactionid.TransactionIDKey] = tid

	resp, err := c.client.PutObject(context.TODO(), s3Param)
	if err != nil {
		log.Errorf("Error found, Resp was : %v", resp)
		return err
	}
	return nil
}

func (c *S3Client2) ListBuckets() (int, error) {
	result, err := c.client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
	if err != nil {
		return 0, err
	}
	return len(result.Buckets), nil
}
