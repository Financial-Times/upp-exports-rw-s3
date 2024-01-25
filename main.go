package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/Financial-Times/upp-exports-rw-s3/service"
	aws2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	s3v2 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/smithy-go/logging"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	cli "github.com/jawher/mow.cli"
	log "github.com/sirupsen/logrus"
)

const (
	spareWorkers = 10 // Workers for things like health check, gtg, count, etc...
)

func main() {
	app := cli.App("upp-exports-rw-s3", "A RESTful API for writing content and concepts to S3")

	appSystemCode := app.String(cli.StringOpt{
		Name:   "app-system-code",
		Value:  "",
		Desc:   "System Code of the application",
		EnvVar: "APP_SYSTEM_CODE",
	})

	port := app.String(cli.StringOpt{
		Name:   "port",
		Value:  "8080",
		Desc:   "Port to listen on",
		EnvVar: "APP_PORT",
	})

	conceptResourcePath := app.String(cli.StringOpt{
		Name:   "conceptResourcePath",
		Value:  "concept",
		Desc:   "Request path parameter to identify a resource, e.g. /concept",
		EnvVar: "CONCEPT_RESOURCE_PATH",
	})

	contentResourcePath := app.String(cli.StringOpt{
		Name:   "contentResourcePath",
		Value:  "content",
		Desc:   "Request path parameter to identify a resource, e.g. /content",
		EnvVar: "CONTENT_RESOURCE_PATH",
	})

	genericStoreResourcePath := app.String(cli.StringOpt{
		Name:   "genericStoreResourcePath",
		Value:  "generic",
		Desc:   "Request path parameter to identify a resource, e.g. /generic",
		EnvVar: "GENERIC_STORE_RESOURCE_PATH",
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

	presignTTL := app.Int(cli.IntOpt{
		Name:   "presignTTL",
		Value:  259200,
		Desc:   "TTL for presign s3 objects",
		EnvVar: "PRESIGN_TTL",
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

	app.Action = func() {
		runServer(*port, *conceptResourcePath, *contentResourcePath, *genericStoreResourcePath, *awsRegion, *bucketName, *bucketContentPrefix, *bucketConceptPrefix, *wrkSize, *appSystemCode, *presignTTL)
	}
	log.SetLevel(log.InfoLevel)
	log.Infof("Application started with args [concept-resource-path: %s] [content-resource-path: %s] [bucketName: %s] [bucketConceptPrefix: %s] [bucketContentPrefix: %s] [workers: %d]", *conceptResourcePath, *contentResourcePath, *bucketName, *bucketConceptPrefix, *bucketContentPrefix, *wrkSize)
	app.Run(os.Args)
}

func runServer(port, conceptResourcePath, contentResourcePath, genericStoreResourcePath, awsRegion, bucketName, bucketContentPrefix, bucketConceptPrefix string, wrks int, appSystemCode string, presignTTL int) {
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

	logger := logging.LoggerFunc(func(classification logging.Classification, format string, v ...interface{}) {
		log.WithField("process", "s3").Info(v...)
	})
	// Assume role
	aws2ConfigAssumeRole, err := config.LoadDefaultConfig(context.TODO(),
		config.WithLogger(logger),
		config.WithHTTPClient(hc), config.WithRegion(awsRegion), config.WithClientLogMode(aws2.LogRetries|aws2.LogRequest|aws2.LogRequestWithBody|aws2.LogResponseWithBody))
	if err != nil {
		log.Fatalf("Failed to create AWS config: %v", err)
	}

	stsSvc := sts.NewFromConfig(aws2ConfigAssumeRole)
	provider := stscreds.NewAssumeRoleProvider(stsSvc, "arn:aws:iam::070529446553:role/cm-foreign-archive-exporter-role")
	aws2ConfigAssumeRole.Credentials = aws2.NewCredentialsCache(provider)
	//_, err = aws2ConfigAssumeRole.Credentials.Retrieve(context.TODO())
	//if err != nil {
	//	log.WithError(err).Error("Error in Credentials.Retrieve")
	//}
	//s3ClientAssumeRole := s3v2.NewFromConfig(aws2ConfigAssumeRole)

	stsSvc2 := sts.NewFromConfig(aws2ConfigAssumeRole)
	provider2 := stscreds.NewAssumeRoleProvider(stsSvc2, "arn:aws:iam::469211898354:role/destination-foreign-exporter-role")
	aws2ConfigAssumeRole.Credentials = aws2.NewCredentialsCache(provider2)

	s3ClientAssumeRole := s3v2.NewFromConfig(aws2ConfigAssumeRole)
	// Test S3 Client Assume Role
	s3v2c := service.NewS3Client2(s3ClientAssumeRole, "destination-test-foreign-archive-exporter")
	testc := []byte("test content")
	err = s3v2c.Write("test-file", &testc, "text/plain", "vs-txt-01")
	if err != nil {
		log.WithError(err).Error("Error write to bucket.")
	}

	aws2Config, err := config.LoadDefaultConfig(context.TODO(), config.WithHTTPClient(hc), config.WithRegion(awsRegion))
	if err != nil {
		log.Fatalf("Failed to create AWS config: %v", err)
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
	credValues, err := sess.Config.Credentials.Get()
	if err != nil {
		log.WithError(err).Fatal("Failed to obtain AWS credentials values")
	}
	log.Infof("Obtaining AWS credentials by using [%s] as provider", credValues.ProviderName)

	svc := s3.New(sess)
	svcV2 := s3v2.NewFromConfig(aws2Config)

	presigner := service.NewPresigner(svcV2, bucketName, presignTTL)
	w := service.NewS3Writer(svc, bucketName, bucketContentPrefix, bucketConceptPrefix)
	r := service.NewS3Reader(svc, bucketName, bucketContentPrefix, bucketConceptPrefix, int16(wrks))

	wh := service.NewWriterHandler(w, r)
	rh := service.NewReaderHandler(r)
	ph := service.NewPresignerHandler(presigner)

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

	genericStoreMethodHandler := &handlers.MethodHandler{
		"PUT":    http.HandlerFunc(wh.HandleGenericStoreWrite),
		"GET":    http.HandlerFunc(rh.HandleGenericStoreGet),
		"DELETE": http.HandlerFunc(wh.HandleGenericStoreDelete),
	}

	presignerMethodHandler := &handlers.MethodHandler{
		"GET": http.HandlerFunc(ph.HandlePresignURL),
	}

	service.Handlers(servicesRouter, contentMethodHandler, contentResourcePath, "/{uuid}")
	service.Handlers(servicesRouter, conceptMethodHandler, conceptResourcePath, "/{fileName}")
	service.Handlers(servicesRouter, genericStoreMethodHandler, genericStoreResourcePath, "/{key}")
	service.Handlers(servicesRouter, presignerMethodHandler, "presign", "/{key}")
	service.AddAdminHandlers(servicesRouter, svc, bucketName, appSystemCode)

	log.Infof("listening on %v", port)

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Unable to start server: %v", err)
	}

}
