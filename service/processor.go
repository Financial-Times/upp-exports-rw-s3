package service

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"

	transactionid "github.com/Financial-Times/transactionid-utils-go"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	log "github.com/sirupsen/logrus"
)

var uuidRegex = regexp.MustCompile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")

type S3QProcessor struct {
	Writer
}
type KafkaMsg struct {
	Id string `json:"uuid"`
}

type Reader interface {
	GetContent(uuid, publishedDate string) (bool, io.ReadCloser, *string, error)
	GetConcept(fileName string) (bool, io.ReadCloser, *string, error)
	GetGenericStore(key string) (bool, io.ReadCloser, *string, error)
	GetPublishDateForUUID(uuid string) (string, bool, error)
}

func NewS3Reader(svc s3iface.S3API, bucketName string, bucketContentPrefix string, bucketConceptPrefix string, workers int16) Reader {
	return &S3Reader{
		svc:                 svc,
		bucketName:          bucketName,
		bucketContentPrefix: bucketContentPrefix,
		bucketConceptPrefix: bucketConceptPrefix,
		workers:             workers,
	}
}

type S3Reader struct {
	svc                 s3iface.S3API
	bucketName          string
	bucketContentPrefix string
	bucketConceptPrefix string
	workers             int16
}

func (r *S3Reader) GetConcept(fileName string) (bool, io.ReadCloser, *string, error) {
	s3ObjectKey := getConceptKey(r.bucketConceptPrefix, fileName)
	return r.Get(s3ObjectKey)
}
func (r *S3Reader) GetContent(uuid, publishedDate string) (bool, io.ReadCloser, *string, error) {
	s3ObjectKey := getContentKey(r.bucketContentPrefix, publishedDate, uuid)
	return r.Get(s3ObjectKey)
}
func (r *S3Reader) GetGenericStore(key string) (bool, io.ReadCloser, *string, error) {
	return r.Get(key)
}

func (r *S3Reader) Get(s3ObjectKey string) (bool, io.ReadCloser, *string, error) {
	s3Param := &s3.GetObjectInput{
		Bucket: aws.String(r.bucketName), // Required
		Key:    aws.String(s3ObjectKey),  // Required
	}

	resp, err := r.svc.GetObject(s3Param)

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
	if r.bucketContentPrefix == "" {
		return &s3.ListObjectsV2Input{
			Bucket: aws.String(r.bucketName),
		}
	}

	return &s3.ListObjectsV2Input{
		Bucket: aws.String(r.bucketName),
		Prefix: aws.String(r.bucketContentPrefix + "/" + uuid),
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

func (r *S3Reader) listObjects(keys chan<- *string, uuid string) error {
	return r.svc.ListObjectsV2Pages(r.getListObjectsV2Input(uuid),
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, o := range page.Contents {
				if !strings.HasSuffix(*o.Key, "/") {
					var key string
					if r.bucketContentPrefix == "" {
						key = *o.Key
					} else {
						k := strings.SplitAfter(*o.Key, r.bucketContentPrefix+"/")
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
	WriteConcept(fileName string, b *[]byte, ct string, tid string) error
	WriteContent(uuid, date string, b *[]byte, contentType string, transactionId string) error
	WriteGenericStore(key string, b *[]byte, contentType string, transactionId string) error
	DeleteContent(uuid, date string) error
	DeleteConcept(fileName string) error
	DeleteGenericStore(key string) error
}

type S3Writer struct {
	svc                 s3iface.S3API
	bucketName          string
	bucketContentPrefix string
	bucketConceptPrefix string
}

func NewS3Writer(svc s3iface.S3API, bucketName string, bucketContentPrefix string, bucketConceptPrefix string) Writer {
	return &S3Writer{
		svc:                 svc,
		bucketName:          bucketName,
		bucketContentPrefix: bucketContentPrefix,
		bucketConceptPrefix: bucketConceptPrefix,
	}
}

func getConceptKey(bucketPrefix, fileName string) string {
	return bucketPrefix + "/" + fileName
}

func getContentKey(bucketPrefix, date, uuid string) string {
	return bucketPrefix + "/" + uuid + "_" + date + ".json"
}

func (w *S3Writer) Delete(s3ObjectKey string) error {
	params := &s3.DeleteObjectInput{
		Bucket: aws.String(w.bucketName), // Required
		Key:    aws.String(s3ObjectKey),  // Required
	}

	if resp, err := w.svc.DeleteObject(params); err != nil {
		log.Errorf("Error found, Resp was : %v", resp)
		return err
	}
	return nil
}

func (w *S3Writer) DeleteConcept(fileName string) error {
	s3ObjectKey := getConceptKey(w.bucketConceptPrefix, fileName)
	return w.Delete(s3ObjectKey)
}

func (w *S3Writer) DeleteContent(uuid, date string) error {
	s3ObjectKey := getContentKey(w.bucketContentPrefix, date, uuid)
	return w.Delete(s3ObjectKey)
}

func (w *S3Writer) DeleteGenericStore(key string) error {
	return w.Delete(key)
}

func (w *S3Writer) WriteConcept(fileName string, b *[]byte, ct string, tid string) error {
	s3Objectkey := getConceptKey(w.bucketConceptPrefix, fileName)
	return w.Write(s3Objectkey, b, ct, tid)
}

func (w *S3Writer) WriteContent(uuid, date string, b *[]byte, ct string, tid string) error {
	s3Objectkey := getContentKey(w.bucketContentPrefix, date, uuid)
	return w.Write(s3Objectkey, b, ct, tid)
}

func (w *S3Writer) WriteGenericStore(key string, b *[]byte, ct string, tid string) error {
	return w.Write(key, b, ct, tid)
}

func (w *S3Writer) Write(s3ObjectKey string, b *[]byte, ct string, tid string) error {
	s3Param := &s3.PutObjectInput{
		Bucket: aws.String(w.bucketName),
		Key:    aws.String(s3ObjectKey),
		Body:   bytes.NewReader(*b),
	}
	if ct != "" {
		s3Param.ContentType = aws.String(ct)
	}

	if s3Param.Metadata == nil {
		s3Param.Metadata = make(map[string]*string)
	}
	s3Param.Metadata[transactionid.TransactionIDKey] = &tid

	resp, err := w.svc.PutObject(s3Param)
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

func (w *WriterHandler) HandleConceptWrite(rw http.ResponseWriter, r *http.Request) {
	fileName := getFileName(r.URL.Path)

	rw.Header().Set("Content-Type", "application/json")
	var err error
	bs, err := ioutil.ReadAll(r.Body)
	if err != nil {
		writerStatusInternalServerError(fileName, err, rw)
		return
	}

	if err != nil {
		writerServiceUnavailable(fileName, err, rw)
		return
	}

	ct := r.Header.Get("Content-Type")
	tid := transactionid.GetTransactionIDFromRequest(r)

	err = w.writer.WriteConcept(fileName, &bs, ct, tid)
	if err != nil {
		writerServiceUnavailable(fileName, err, rw)
		return
	}

	rw.WriteHeader(http.StatusOK)
}

func (w *WriterHandler) HandleConceptDelete(rw http.ResponseWriter, r *http.Request) {
	fileName := getFileName(r.URL.Path)
	found, _, _, err := w.reader.GetConcept(fileName)
	if err != nil {
		rw.Header().Set("Content-Type", "application/json")
		writerServiceUnavailable(fileName, err, rw)
		return
	}

	if !found {
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusNotFound)
		rw.Write([]byte("{\"message\":\"Item not found\"}"))
		return
	}

	if err := w.writer.DeleteConcept(fileName); err != nil {
		rw.Header().Set("Content-Type", "application/json")
		writerServiceUnavailable(fileName, err, rw)
		return
	}

	log.WithField("fileName", fileName).Info("Delete succesful")
	rw.WriteHeader(http.StatusNoContent)
}

func respondWithBadRequest(rw http.ResponseWriter, message string) {
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(http.StatusBadRequest)
	rw.Write([]byte(fmt.Sprintf("{\"message\":\"%s\"}", message)))
}

func (rh *ReaderHandler) HandleGenericStoreGet(rw http.ResponseWriter, r *http.Request) {
	key := getFileName(r.URL.Path)
	f, i, ct, err := rh.reader.GetGenericStore(key)
	if err != nil {
		readerServiceUnavailable(r.URL.RequestURI(), err, rw)
		return
	}
	handleGet(f, rw, i, ct)
}

func (rh *ReaderHandler) HandleContentGet(rw http.ResponseWriter, r *http.Request) {
	uuid := getFileName(r.URL.Path)
	if isUuidValid := uuidRegex.MatchString(uuid); !isUuidValid {
		respondWithBadRequest(rw, "Provided UUID is invalid.")
		return
	}

	publishedDate := r.URL.Query().Get("date")
	if publishedDate == "" {
		respondWithBadRequest(rw, "Required query param 'date' was not provided.")
		return
	}

	f, i, ct, err := rh.reader.GetContent(uuid, publishedDate)
	if err != nil {
		readerServiceUnavailable(r.URL.RequestURI(), err, rw)
		return
	}

	handleGet(f, rw, i, ct)
}

func (rh *ReaderHandler) HandleConceptGet(rw http.ResponseWriter, r *http.Request) {
	fileName := getFileName(r.URL.Path)

	f, i, ct, err := rh.reader.GetConcept(fileName)
	if err != nil {
		readerServiceUnavailable(r.URL.RequestURI(), err, rw)
		return
	}

	handleGet(f, rw, i, ct)
}

func handleGet(f bool, rw http.ResponseWriter, i io.ReadCloser, ct *string) {
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

func (w *WriterHandler) HandleGenericStoreWrite(rw http.ResponseWriter, r *http.Request) {
	key := getFileName(r.URL.Path)
	ct := r.Header.Get("Content-Type")
	rw.Header().Set("Content-Type", ct)

	var err error
	bs, err := io.ReadAll(r.Body)
	if err != nil {
		writerStatusInternalServerError(key, err, rw)
		return
	}

	tid := transactionid.GetTransactionIDFromRequest(r)
	err = w.writer.WriteGenericStore(key, &bs, ct, tid)
	if err != nil {
		writerServiceUnavailable("", err, rw)
		return
	}
}

func (w *WriterHandler) HandleContentWrite(rw http.ResponseWriter, r *http.Request) {
	uuid := getFileName(r.URL.Path)
	if isUuidValid := uuidRegex.MatchString(uuid); !isUuidValid {
		respondWithBadRequest(rw, "Provided UUID is invalid.")
		return
	}

	newPublishDate := r.URL.Query().Get("date")
	if newPublishDate == "" {
		respondWithBadRequest(rw, "Required query param 'date' was not provided.")
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

	err = w.writer.WriteContent(uuid, newPublishDate, &bs, ct, tid)
	if err != nil {
		writerServiceUnavailable(uuid, err, rw)
		return
	}

	if found && newPublishDate != oldPublishDate {
		err = w.writer.DeleteContent(uuid, oldPublishDate)
		if err != nil {
			//try to revert the update
			w.writer.DeleteContent(uuid, newPublishDate)
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

func (w *WriterHandler) HandleGenericStoreDelete(rw http.ResponseWriter, r *http.Request) {
	key := getFileName(r.URL.Path)

	found, _, _, err := w.reader.GetGenericStore(key)
	if err != nil {
		rw.Header().Set("Content-Type", "application/json")
		writerServiceUnavailable(key, err, rw)
		return
	}

	if !found {
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusNotFound)
		rw.Write([]byte("{\"message\":\"Item not found\"}"))
		return
	}

	if err := w.writer.DeleteGenericStore(key); err != nil {
		rw.Header().Set("Content-Type", "application/json")
		writerServiceUnavailable(key, err, rw)
		return
	}

	log.WithField("key", key).Info("Delete succesful")
	rw.WriteHeader(http.StatusNoContent)
}

func (w *WriterHandler) HandleContentDelete(rw http.ResponseWriter, r *http.Request) {
	uuid := getFileName(r.URL.Path)
	if isUuidValid := uuidRegex.MatchString(uuid); !isUuidValid {
		respondWithBadRequest(rw, "Provided UUID is invalid.")
		return
	}

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

	if err := w.writer.DeleteContent(uuid, publishedDate); err != nil {
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

func getFileName(path string) string {
	parts := strings.Split(path, "/")
	return parts[len(parts)-1]
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
