package service

import (
	"fmt"
	"net/http"
	"time"

	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
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

	hc := &fthealth.TimedHealthCheck{
		HealthCheck: fthealth.HealthCheck{
			SystemCode:  "test",
			Name:        "UppExportsReadWriteS3 Healthchecks",
			Description: "Runs a HEAD check on bucket",
			Checks: []fthealth.Check{
				{
					BusinessImpact:   "Unable to access S3 bucket",
					Name:             "S3 Bucket check",
					PanicGuide:       "https://runbooks.ftops.tech/upp-exports-rw-s3",
					Severity:         2,
					TechnicalSummary: `Can not access S3 bucket.`,
					Checker:          c.healthCheck,
				},
			},
		},
		Timeout: 10 * time.Second,
	}

	http.HandleFunc("/__health", fthealth.Handler(hc))

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
