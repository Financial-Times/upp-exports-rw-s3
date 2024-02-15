package service

import (
	"context"
	"io/ioutil"
	"net/http"

	transactionid "github.com/Financial-Times/transactionid-utils-go"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	log "github.com/sirupsen/logrus"
)

type Foreigner struct {
	httpClient *http.Client
	roles      []string
	bucketName string
	awsRegion  string
	s3c        *S3Client2
}

func NewForeigner(httpClient *http.Client, roles []string, bucketName, awsRegion string) *Foreigner {
	return &Foreigner{
		httpClient: httpClient,
		roles:      roles,
		bucketName: bucketName,
		awsRegion:  awsRegion,
	}
}

func (f *Foreigner) UploadToBucket(key string, b *[]byte, ct string, tid string) error {
	if err := f.loadS3Client(); err != nil {
		return err
	}
	return f.s3c.Write(key, b, ct, tid)
}

func (f *Foreigner) loadS3Client() error {
	cfg, err := f.loadMultipleRolesAwsConfiguration()
	if err != nil {
		return err
	}
	aws3s := s3.NewFromConfig(*cfg)
	f.s3c = NewS3Client2(aws3s, f.bucketName)
	return nil
}

// Assume every next role
func (f *Foreigner) loadMultipleRolesAwsConfiguration() (*aws.Config, error) {
	var cfg *aws.Config
	for _, role := range f.roles {
		var err error
		cfg, err = f.loadAwsConfiguration(cfg, role)
		if err != nil {
			return nil, err
		}
	}
	return cfg, nil
}

// Load AWS Configuration
// Use provided cfg if needed or create new
func (f *Foreigner) loadAwsConfiguration(cfg *aws.Config, role string) (*aws.Config, error) {
	if cfg == nil {
		_cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithHTTPClient(f.httpClient), config.WithRegion(f.awsRegion))
		cfg = &_cfg
		if err != nil {
			return nil, err
		}
	}
	stsSvc := sts.NewFromConfig(*cfg)
	provider := stscreds.NewAssumeRoleProvider(stsSvc, role)
	cfg.Credentials = aws.NewCredentialsCache(provider)
	return cfg, nil
}

type ForeignerHandler struct {
	httpClient *http.Client
}

func NewForeignerHandler(httpClient *http.Client) ForeignerHandler {
	return ForeignerHandler{httpClient}
}

func (h *ForeignerHandler) HandleForeignerBucketWrite(rw http.ResponseWriter, r *http.Request) {
	// params: region, role, region, bucketName, key
	region := r.URL.Query().Get("region")
	roles := r.URL.Query()["role"]
	bucket := r.URL.Query().Get("bucket")
	key := r.URL.Query().Get("key")

	// New Foreigner
	foreigner := NewForeigner(h.httpClient, roles, bucket, region)

	ct := r.Header.Get("Content-Type")
	bs, err := ioutil.ReadAll(r.Body)
	if err != nil {
		foreignerServiceUnavailable(bucket, err, rw)
		return
	}
	tid := transactionid.GetTransactionIDFromRequest(r)

	if err = foreigner.UploadToBucket(key, &bs, ct, tid); err != nil {
		foreignerServiceUnavailable(bucket, err, rw)
		return
	}
	rw.WriteHeader(http.StatusCreated)
}

func foreignerServiceUnavailable(bucketName string, err error, rw http.ResponseWriter) {
	log.WithError(err).WithField("bucketName", bucketName).Error("Error writing object")
	rw.Header().Set("Content-Type", "application/json")
	respondServiceUnavailable(err, rw)
}
