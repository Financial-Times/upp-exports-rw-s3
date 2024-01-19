package service

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go/aws"
)

type presignurl struct {
	URL string `json:"url"`
}

type Presigner struct {
	PresignClient *s3.PresignClient
	bucketName    string
	ttl           int
}

func NewPresigner(s3Client *s3.Client, bucketName string, ttl int) *Presigner {
	presignClient := s3.NewPresignClient(s3Client)
	return &Presigner{
		PresignClient: presignClient,
		bucketName:    bucketName,
		ttl:           ttl}
}

func (p *Presigner) GetPresignURL(key string) (string, error) {
	presignedGetRequest, err := p.getObject(p.bucketName, key, int64(p.ttl))
	if err != nil {
		return "", err
	}
	return presignedGetRequest.URL, nil
}

// GetObject makes a presigned request that can be used to get an object from a bucket.
// The presigned request is valid for the specified number of seconds.
func (presigner Presigner) getObject(
	bucketName string, objectKey string, lifetimeSecs int64) (*v4.PresignedHTTPRequest, error) {
	request, err := presigner.PresignClient.PresignGetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	}, func(opts *s3.PresignOptions) {
		opts.Expires = time.Duration(lifetimeSecs * int64(time.Second))
	})
	if err != nil {
		return nil, err
	}
	return request, err
}

type PresignerHandler struct {
	presigner *Presigner
}

func NewPresignerHandler(presigner *Presigner) PresignerHandler {
	return PresignerHandler{
		presigner: presigner,
	}
}

func (h *PresignerHandler) HandlePresignURL(rw http.ResponseWriter, r *http.Request) {
	key := getFileName(r.URL.Path)
	purl, err := h.presigner.GetPresignURL(key)
	if err != nil {
		respondServiceUnavailable(err, rw)
		return
	}
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(http.StatusOK)
	json.NewEncoder(rw).Encode(presignurl{purl})
}
