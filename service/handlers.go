package service

import (
	"fmt"
	"net/http"

	"github.com/Financial-Times/go-fthealth/v1a"
	"github.com/Financial-Times/http-handlers-go/httphandlers"
	"github.com/Financial-Times/service-status-go/gtg"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"
)

func AddAdminHandlers(servicesRouter *mux.Router, svc s3iface.S3API, bucketName string) {
	c := checker{svc, bucketName}
	var monitoringRouter http.Handler = servicesRouter
	monitoringRouter = httphandlers.TransactionAwareRequestLoggingHandler(log.StandardLogger(), monitoringRouter)
	monitoringRouter = httphandlers.HTTPMetricsHandler(metrics.DefaultRegistry, monitoringRouter)
	http.HandleFunc(status.PingPath, status.PingHandler)
	http.HandleFunc(status.PingPathDW, status.PingHandler)
	http.HandleFunc(status.BuildInfoPath, status.BuildInfoHandler)
	http.HandleFunc(status.BuildInfoPathDW, status.BuildInfoHandler)
	http.HandleFunc("/__health", v1a.Handler("UppExportsReadWriteS3 Healthchecks",
		"Runs a HEAD check on bucket", v1a.Check{
			BusinessImpact:   "Unable to access S3 bucket",
			Name:             "S3 Bucket check",
			PanicGuide:       "https://runbooks.ftops.tech/upp-exports-rw-s3",
			Severity:         2,
			TechnicalSummary: `Can not access S3 bucket.`,
			Checker:          c.healthCheck,
		}))

	gtgHandler := status.NewGoodToGoHandler(c.gtgCheckHandler)
	http.HandleFunc(status.GTGPath, gtgHandler)
	http.Handle("/", monitoringRouter)
}

type checker struct {
	s3iface.S3API
	bucketName string
}

func (c *checker) healthCheck() (string, error) {
	params := &s3.HeadBucketInput{
		Bucket: aws.String(c.bucketName), // Required
	}

	_, err := c.HeadBucket(params)
	if err != nil {
		log.Errorf("Got error running S3 health check, %v", err.Error())
		return "Can not perform check on S3 bucket", err
	}

	return "Access to S3 bucket ok", err
}

func (c *checker) gtgCheckHandler() gtg.Status {
	if _, err := c.healthCheck(); err != nil {
		log.Info("Healthcheck failed, gtg is bad.")
		return gtg.Status{GoodToGo: false, Message: "Head request to S3 failed"}
	}

	return gtg.Status{GoodToGo: true, Message: "OK"}
}

func Handlers(servicesRouter *mux.Router, mh *handlers.MethodHandler, resourcePath string, endpointRegex string) {
	if resourcePath != "" {
		resourcePath = fmt.Sprintf("/%s", resourcePath)
	}

	servicesRouter.Handle(fmt.Sprintf("%s%s", resourcePath, endpointRegex), mh)
}
