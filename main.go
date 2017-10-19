package main

import (
	"net"
	"net/http"
	"os"
	"time"

	"github.com/Financial-Times/base-ft-rw-app-go/baseftrwapp"
	"github.com/Financial-Times/upp-exports-rw-s3/service"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gorilla/mux"
	"github.com/jawher/mow.cli"
	log "github.com/sirupsen/logrus"
	"github.com/gorilla/handlers"
)

const (
	spareWorkers = 10 // Workers for things like health check, gtg, count, etc...
)

func main() {
	app := cli.App("content-rw-s3", "A RESTful API for writing data to S3")

	port := app.String(cli.StringOpt{
		Name:   "port",
		Value:  "8080",
		Desc:   "Port to listen on",
		EnvVar: "APP_PORT",
	})

	conceptResourcePath := app.String(cli.StringOpt{
		Name:   "conceptResourcePath",
		Value:  "",
		Desc:   "Request path parameter to identify a resource, e.g. /concept",
		EnvVar: "CONCEPT_RESOURCE_PATH",
	})

	contentResourcePath := app.String(cli.StringOpt{
		Name:   "contentResourcePath",
		Value:  "",
		Desc:   "Request path parameter to identify a resource, e.g. /content",
		EnvVar: "CONTENT_RESOURCE_PATH",
	})

	awsRegion := app.String(cli.StringOpt{
		Name:   "awsRegion",
		Value:  "eu-west-1",
		Desc:   "AWS Region to connect to",
		EnvVar: "AWS_REGION",
	})

	bucketName := app.String(cli.StringOpt{
		Name:   "bucketName",
		Value:  "",
		Desc:   "Bucket name to upload things to",
		EnvVar: "BUCKET_NAME",
	})

	bucketContentPrefix := app.String(cli.StringOpt{
		Name:   "bucketContentPrefix",
		Value:  "",
		Desc:   "Prefix for content going into S3 bucket",
		EnvVar: "BUCKET_CONTENT_PREFIX",
	})

	bucketConceptPrefix := app.String(cli.StringOpt{
		Name:   "bucketConceptPrefix",
		Value:  "",
		Desc:   "Prefix for concepts going into S3 bucket",
		EnvVar: "BUCKET_CONCEPT_PREFIX",
	})

	wrkSize := app.Int(cli.IntOpt{
		Name:   "workers",
		Value:  10,
		Desc:   "Number of workers to use when batch downloading",
		EnvVar: "WORKERS",
	})

	graphiteTCPAddress := app.String(cli.StringOpt{
		Name:   "graphiteTCPAddress",
		Value:  "",
		Desc:   "Graphite TCP address, e.g. graphite.ft.com:2003. Leave as default if you do NOT want to output to graphite (e.g. if running locally",
		EnvVar: "GRAPHITE_ADDRESS",
	})

	graphitePrefix := app.String(cli.StringOpt{
		Name:   "graphitePrefix",
		Value:  "",
		Desc:   "Prefix to use. Should start with content, include the environment, and the host name. e.g. coco.pre-prod.service-name.1 or content.test.people.rw.service-name.ftaps58938-law1a-eu-t",
		EnvVar: "GRAPHITE_PREFIX",
	})

	logMetrics := app.Bool(cli.BoolOpt{
		Name:   "logMetrics",
		Value:  false,
		Desc:   "Whether to log metrics. Set to true if running locally and you want metrics output",
		EnvVar: "LOG_METRICS",
	})

	app.Action = func() {
		baseftrwapp.OutputMetricsIfRequired(*graphiteTCPAddress, *graphitePrefix, *logMetrics)
		runServer(*port, *conceptResourcePath, *contentResourcePath, *awsRegion, *bucketName, *bucketContentPrefix, *bucketConceptPrefix, *wrkSize)
	}
	log.SetLevel(log.InfoLevel)
	log.Infof("Application started with args [concept-resource-path: %s] [content-resource-path: %s] [bucketName: %s] [bucketConceptPrefix: %s] [bucketContentPrefix: %s] [workers: %d]", *conceptResourcePath, *contentResourcePath, *bucketName, *bucketConceptPrefix, *bucketContentPrefix, *wrkSize)
	app.Run(os.Args)
}

func runServer(port string, conceptResourcePath string, contentResourcePath string, awsRegion string, bucketName string, bucketContentPrefix string, bucketConceptPrefix string, wrks int) {
	hc := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          wrks + spareWorkers,
			IdleConnTimeout:       90 * time.Second,
			MaxIdleConnsPerHost:   wrks + spareWorkers,
			TLSHandshakeTimeout:   3 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
	sess, err := session.NewSession(
		&aws.Config{
			Region:     aws.String(awsRegion),
			MaxRetries: aws.Int(1),
			HTTPClient: hc,
		})
	if err != nil {
		log.Fatalf("Failed to create AWS session: %v", err)
	}
	svc := s3.New(sess)

	w := service.NewS3Writer(svc, bucketName, bucketContentPrefix, bucketConceptPrefix)
	r := service.NewS3Reader(svc, bucketName, bucketContentPrefix, bucketConceptPrefix, int16(wrks))

	wh := service.NewWriterHandler(w, r)
	rh := service.NewReaderHandler(r)

	servicesRouter := mux.NewRouter()

	contentMethodHandler := &handlers.MethodHandler{
		"PUT":    http.HandlerFunc(wh.HandleContentWrite),
		"GET":    http.HandlerFunc(rh.HandleContentGet),
		"DELETE": http.HandlerFunc(wh.HandleContentDelete),
	}

	conceptMethodHandler := &handlers.MethodHandler{
		"PUT":    http.HandlerFunc(wh.HandleConceptWrite),
		"GET":    http.HandlerFunc(rh.HandleConceptGet),
		"DELETE": http.HandlerFunc(wh.HandleConceptDelete),
	}

	service.Handlers(servicesRouter, contentMethodHandler, contentResourcePath, "/{uuid}")
	service.Handlers(servicesRouter, conceptMethodHandler, conceptResourcePath, "/{fileName}")
	service.AddAdminHandlers(servicesRouter, svc, bucketName)

	log.Infof("listening on %v", port)

	if err := http.ListenAndServe(":" + port, nil); err != nil {
		log.Fatalf("Unable to start server: %v", err)
	}

}
