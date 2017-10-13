package service

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	transactionid "github.com/Financial-Times/transactionid-utils-go"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	log "github.com/sirupsen/logrus"
	"fmt"
)

type S3QProcessor struct {
	Writer
}
type KafkaMsg struct {
	Id string `json:"uuid"`
}

type Reader interface {
	Get(uuid, publishedDate string) (bool, io.ReadCloser, *string, error)
	GetPublishDateForUUID(uuid string) (string, bool, error)
}

func NewS3Reader(svc s3iface.S3API, bucketName string, bucketPrefix string, workers int16) Reader {
	return &S3Reader{
		svc:          svc,
		bucketName:   bucketName,
		bucketPrefix: bucketPrefix,
		workers:      workers,
	}
}

type S3Reader struct {
	svc          s3iface.S3API
	bucketName   string
	bucketPrefix string
	workers      int16
}

func (r *S3Reader) Get(uuid, publishedDate string) (bool, io.ReadCloser, *string, error) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(r.bucketName), // Required
		Key:    aws.String(getKey(r.bucketPrefix, publishedDate, uuid)), // Required
	}
	resp, err := r.svc.GetObject(params)

	if err != nil {
		e, ok := err.(awserr.Error)
		if ok && e.Code() == "NoSuchKey" {
			return false, nil, nil, nil
		}
		return false, nil, nil, err
	}

	return true, resp.Body, resp.ContentType, err
}

func (r *S3Reader) getListObjectsV2Input(uuid string) *s3.ListObjectsV2Input {
	if r.bucketPrefix == "" {
		return &s3.ListObjectsV2Input{
			Bucket: aws.String(r.bucketName),
		}
	}
	return &s3.ListObjectsV2Input{
		Bucket: aws.String(r.bucketName),
		Prefix: aws.String(r.bucketPrefix + "/" + uuid),
	}
}

func (r *S3Reader) GetPublishDateForUUID(uuid string) (string, bool, error) {
	keys := make(chan *string, 3000)
	go r.listObjects(keys, uuid)

	for key := range keys {
		splitKey := strings.Split(*key, "_")
		if len(splitKey) < 2 {
			return "", false, fmt.Errorf("Cannot parse date from s3 object key %s", *key)
		}

		if splitKey[0] == uuid {
			return splitKey[1], true, nil
		}
	}

	return "", false, nil
}

func (r *S3Reader) listObjects(keys chan <- *string, uuid string) error {
	return r.svc.ListObjectsV2Pages(r.getListObjectsV2Input(uuid),
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, o := range page.Contents {
				if !strings.HasSuffix(*o.Key, "/") {
					var key string
					if r.bucketPrefix == "" {
						key = *o.Key
					} else {
						k := strings.SplitAfter(*o.Key, r.bucketPrefix + "/")
						key = k[1]
					}
					uuid := strings.TrimSuffix(key, ".json")
					keys <- &uuid
				}
			}

			if lastPage {
				close(keys)
			}

			return !lastPage
		})
}

type Writer interface {
	Write(uuid, date string, b *[]byte, contentType string, transactionId string) error
	Delete(uuid, date string) error
}

type S3Writer struct {
	svc          s3iface.S3API
	bucketName   string
	bucketPrefix string
}

func NewS3Writer(svc s3iface.S3API, bucketName string, bucketPrefix string) Writer {
	return &S3Writer{
		svc:          svc,
		bucketName:   bucketName,
		bucketPrefix: bucketPrefix,
	}
}

func getKey(bucketPrefix, date, uuid string) string {
	return bucketPrefix + "/" + uuid + "_" + date + ".json"
}

func (w *S3Writer) Delete(uuid, date string) error {
	params := &s3.DeleteObjectInput{
		Bucket: aws.String(w.bucketName), // Required
		Key:    aws.String(getKey(w.bucketPrefix, date, uuid)), // Required
	}

	if resp, err := w.svc.DeleteObject(params); err != nil {
		log.Errorf("Error found, Resp was : %v", resp)
		return err
	}
	return nil
}

func (w *S3Writer) Write(uuid, date string, b *[]byte, ct string, tid string) error {
	params := &s3.PutObjectInput{
		Bucket: aws.String(w.bucketName),
		Key:    aws.String(getKey(w.bucketPrefix, date, uuid)),
		Body:   bytes.NewReader(*b),
	}

	if ct != "" {
		params.ContentType = aws.String(ct)
	}

	if params.Metadata == nil {
		params.Metadata = make(map[string]*string)
	}
	params.Metadata[transactionid.TransactionIDKey] = &tid

	resp, err := w.svc.PutObject(params)

	if err != nil {
		log.Errorf("Error found, Resp was : %v", resp)
		return err
	}
	return nil
}

type WriterHandler struct {
	writer Writer
	reader Reader
}

func NewWriterHandler(writer Writer, reader Reader) WriterHandler {
	return WriterHandler{
		writer: writer,
		reader: reader,
	}
}

func (w *WriterHandler) HandleWrite(rw http.ResponseWriter, r *http.Request) {
	uuid := uuid(r.URL.Path)
	newPublishDate := r.URL.Query().Get("date")

	if newPublishDate == "" {
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusBadRequest)
		rw.Write([]byte("{\"message\":\"Required query param 'date' was not provided.\"}"))
		return
	}

	rw.Header().Set("Content-Type", "application/json")
	var err error
	bs, err := ioutil.ReadAll(r.Body)

	if err != nil {
		writerStatusInternalServerError(uuid, err, rw)
		return
	}

	oldPublishDate, found, err := w.reader.GetPublishDateForUUID(uuid)
	if err != nil {
		writerServiceUnavailable(uuid, err, rw)
		return
	}
	ct := r.Header.Get("Content-Type")
	tid := transactionid.GetTransactionIDFromRequest(r)

	err = w.writer.Write(uuid, newPublishDate, &bs, ct, tid)
	if err != nil {
		writerServiceUnavailable(uuid, err, rw)
		return
	}

	if found && newPublishDate != oldPublishDate {
		err = w.writer.Delete(uuid, oldPublishDate)
		if err != nil {
			//try to revert the update
			w.writer.Delete(uuid, newPublishDate)
			writerServiceUnavailable(uuid, err, rw)
			return
		}
	}

	if found {
		rw.WriteHeader(http.StatusOK)
		rw.Write([]byte("{\"message\":\"UPDATED\"}"))
	} else {
		rw.WriteHeader(http.StatusCreated)
		rw.Write([]byte("{\"message\":\"CREATED\"}"))
	}
}

func writerStatusInternalServerError(uuid string, err error, rw http.ResponseWriter) {
	log.WithError(err).WithField("UUID", uuid).Error("Error writing object")
	rw.WriteHeader(http.StatusInternalServerError)
	rw.Write([]byte("{\"message\":\"Unknown internal error\"}"))
}

func (w *WriterHandler) HandleDelete(rw http.ResponseWriter, r *http.Request) {
	uuid := uuid(r.URL.Path)
	publishedDate, found, err := w.reader.GetPublishDateForUUID(uuid)

	if err != nil {
		rw.Header().Set("Content-Type", "application/json")
		writerServiceUnavailable(uuid, err, rw)
		return
	}

	if !found {
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusNotFound)
		rw.Write([]byte("{\"message\":\"Item not found\"}"))
		return
	}

	if err := w.writer.Delete(uuid, publishedDate); err != nil {
		rw.Header().Set("Content-Type", "application/json")
		writerServiceUnavailable(uuid, err, rw)
		return
	}

	log.WithField("UUID", uuid).Info("Delete succesful")
	rw.WriteHeader(http.StatusNoContent)
}

func NewReaderHandler(reader Reader) ReaderHandler {
	return ReaderHandler{reader: reader}
}

type ReaderHandler struct {
	reader Reader
}

func (rh *ReaderHandler) HandleGet(rw http.ResponseWriter, r *http.Request) {
	uuid := uuid(r.URL.Path)
	publishedDate := r.URL.Query().Get("date")

	if publishedDate == "" {
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusBadRequest)
		rw.Write([]byte("{\"message\":\"Required query param 'date' was not provided.\"}"))
		return
	}

	f, i, ct, err := rh.reader.Get(uuid, publishedDate)
	if err != nil {
		readerServiceUnavailable(r.URL.RequestURI(), err, rw)
		return
	}
	if !f {
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusNotFound)
		rw.Write([]byte("{\"message\":\"Item not found\"}"))
		return
	}

	b, err := ioutil.ReadAll(i)

	if err != nil {
		log.WithError(err).Error("Error reading body")
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusBadGateway)
		rw.Write([]byte("{\"message\":\"Error while communicating to other service\"}"))
		return
	}

	if ct != nil || *ct != "" {
		rw.Header().Set("Content-Type", *ct)
	}

	rw.WriteHeader(http.StatusOK)
	rw.Write(b)
}

func uuid(path string) string {
	parts := strings.Split(path, "/")
	return parts[len(parts) - 1]
}

func respondServiceUnavailable(err error, rw http.ResponseWriter) {
	e, ok := err.(awserr.Error)
	if ok {
		errorCode := e.Code()
		log.Errorf("Response from S3. %s. More info %s ",
			errorCode, "https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html")
	}
	rw.WriteHeader(http.StatusServiceUnavailable)
	rw.Write([]byte("{\"message\":\"Service currently unavailable\"}"))
}

func writerServiceUnavailable(uuid string, err error, rw http.ResponseWriter) {
	log.WithError(err).WithField("UUID", uuid).Error("Error writing object")
	respondServiceUnavailable(err, rw)
}

func readerServiceUnavailable(requestURI string, err error, rw http.ResponseWriter) {
	log.WithError(err).WithField("requestURI", requestURI).Error("Error from reader")
	rw.Header().Set("Content-Type", "application/json")
	respondServiceUnavailable(err, rw)
}
