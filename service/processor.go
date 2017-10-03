package service

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/Financial-Times/message-queue-gonsumer/consumer"
	transactionid "github.com/Financial-Times/transactionid-utils-go"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	log "github.com/sirupsen/logrus"
	"fmt"
)

type QProcessor interface {
	ProcessMsg(m consumer.Message)
}

func NewQProcessor(w Writer) QProcessor {
	return &S3QProcessor{w}
}

type S3QProcessor struct {
	Writer
}
type KafkaMsg struct {
	Id string `json:"uuid"`
}

func (r *S3QProcessor) ProcessMsg(m consumer.Message) {
	var uuid string
	var ct string
	var ok bool
	tid := m.Headers[transactionid.TransactionIDHeader]
	if tid == "" {
		tid = transactionid.NewTransactionID()
	}
	if ct, ok = m.Headers["Content-Type"]; !ok {
		ct = ""
	}

	var km KafkaMsg
	b := []byte(m.Body)
	if err := json.Unmarshal(b, &km); err != nil {
		log.WithError(err).WithFields(log.Fields{
			"message_id":     m.Headers["Message-Id"],
			"transaction_id": tid,
		}).Error("Could not unmarshal message")
		return
	}

	if uuid = km.Id; uuid == "" {
		uuid = m.Headers["Message-Id"]
	}

	if err := r.Write(uuid, "TODO", &b, ct, tid); err != nil {
		log.WithError(err).WithFields(log.Fields{
			"UUID":           uuid,
			"transaction_id": tid,
		}).Error("Failed to write")
	} else {
		log.WithError(err).WithFields(log.Fields{
			"UUID":           uuid,
			"transaction_id": tid,
		}).Info("Wrote successfully")
	}
}

type Reader interface {
	Get(uuid, publishedDate string) (bool, io.ReadCloser, *string, error)
	Count() (int64, error)
	Ids() (io.PipeReader, error)
	GetAll() (io.PipeReader, error)
	Head(uuid, date string) (bool, error)
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

func (r *S3Reader) Head(uuid, date string) (bool, error) {
	params := &s3.HeadObjectInput{
		Bucket: aws.String(r.bucketName), // Required
		Key:    aws.String(getKey(r.bucketPrefix, date, uuid)), // Required
	}

	_, err := r.svc.HeadObject(params)
	if err != nil {
		e, ok := err.(awserr.Error)
		if ok && e.Code() == "NotFound" {
			return false, nil
		}
		log.Errorf("Error found : %v", err.Error())
		return false, err
	}
	return true, nil
}

func (r *S3Reader) Count() (int64, error) {
	cc := make(chan *s3.ListObjectsV2Output, 10)
	rc := make(chan int64, 1)

	go func() {
		t := int64(0)
		for i := range cc {
			for _, o := range i.Contents {
				if (!strings.HasSuffix(*o.Key, "/") && !strings.HasPrefix(*o.Key, "__")) && (*o.Key != ".") {
					t++
				}
			}
		}
		rc <- t
	}()

	err := r.svc.ListObjectsV2Pages(r.getListObjectsV2Input(),
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			cc <- page

			if lastPage {
				close(cc)
			}
			return !lastPage
		})

	var c int64
	if err == nil {
		c = <-rc
	} else {
		close(rc)
	}
	return c, err
}

func (r *S3Reader) getListObjectsV2Input() *s3.ListObjectsV2Input {
	if r.bucketPrefix == "" {
		return &s3.ListObjectsV2Input{
			Bucket: aws.String(r.bucketName),
		}
	}
	return &s3.ListObjectsV2Input{
		Bucket: aws.String(r.bucketName),
		Prefix: aws.String(r.bucketPrefix + "/"),
	}
}

func (r *S3Reader) GetPublishDateForUUID(uuid string) (string, bool, error) {
	keys := make(chan *string, 3000)
	go r.listObjects(keys)

	for key := range keys {
		splitKey := strings.Split(*key, "_")
		if len(splitKey) < 2 {
			return "", false, fmt.Errorf("Cannot parse date from s3 object key %s", *key)
		}

		if splitKey[1] == uuid {
			return splitKey[0], true, nil
		}
	}

	return "", false, nil
}

func (r *S3Reader) GetAll() (io.PipeReader, error) {
	err := r.checkListOk()
	pv, pw := io.Pipe()
	if err != nil {
		pv.Close()
		return *pv, err
	}

	itemSize := float32(r.workers) * 1.5
	items := make(chan *io.ReadCloser, int(itemSize))
	keys := make(chan *string, 3000) //  Three times the default Page size
	go r.processItems(items, pw)
	var wg sync.WaitGroup
	tw := int(r.workers)
	for w := 0; w < tw; w++ {
		wg.Add(1)
		go r.getItemWorker(w, &wg, keys, items)
	}

	go r.listObjects(keys)

	go func(w *sync.WaitGroup, i chan *io.ReadCloser) {
		w.Wait()
		close(i)
	}(&wg, items)

	return *pv, err
}

func (r *S3Reader) getItemWorker(w int, wg *sync.WaitGroup, keys <-chan *string, items chan <- *io.ReadCloser) {
	defer wg.Done()
	for uuidWithPD := range keys {
		splitted := strings.Split(*uuidWithPD, "_")
		//todo: fix this, splitted[0] is the publishDate and splitted[1] is uuid, r.get() takes uuid as first param, and publishDate as second param.
		if found, i, _, _ := r.Get(splitted[0], splitted[1]); found {
			items <- &i
		}
	}
}

func (r *S3Reader) processItems(items <-chan *io.ReadCloser, pw *io.PipeWriter) {
	for item := range items {
		if _, err := io.Copy(pw, *item); err != nil {
			log.Errorf("Error reading from S3: %v", err.Error())
		} else {
			io.WriteString(pw, "\n")
		}
	}
	pw.Close()
}

func (r *S3Reader) Ids() (io.PipeReader, error) {

	err := r.checkListOk()
	pv, pw := io.Pipe()
	if err != nil {
		pv.Close()
		return *pv, err
	}

	go func(p *io.PipeWriter) {

		keys := make(chan *string, 3000) //  Three times the default Page size
		go func(c <-chan *string, out *io.PipeWriter) {
			encoder := json.NewEncoder(out)
			for key := range c {
				pl := obj{UUID: *key}
				if err := encoder.Encode(pl); err != nil {
					log.Errorf("Got error encoding key : %v", err.Error())
					break
				}
			}
			out.Close()
		}(keys, p)

		err := r.listObjects(keys)
		if err != nil {
			log.Errorf("Got an error reading content of bucket : %v", err.Error())
		}
	}(pw)
	return *pv, err
}

func (r *S3Reader) checkListOk() (err error) {
	p := r.getListObjectsV2Input()
	p.MaxKeys = aws.Int64(1)
	_, err = r.svc.ListObjectsV2(p)
	return err
}

func (r *S3Reader) listObjects(keys chan <- *string) error {
	return r.svc.ListObjectsV2Pages(r.getListObjectsV2Input(),
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, o := range page.Contents {
				if (!strings.HasSuffix(*o.Key, "/") && !strings.HasPrefix(*o.Key, "__")) && (*o.Key != ".") {
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
	return bucketPrefix + "/" + date + "_" + uuid + ".json"
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

func (rh *ReaderHandler) HandleIds(rw http.ResponseWriter, r *http.Request) {
	pv, err := rh.reader.Ids()
	defer pv.Close()
	if err != nil {
		readerServiceUnavailable(r.URL.RequestURI(), err, rw)
		return
	}

	rw.Header().Set("Content-Type", "application/octet-stream")
	rw.WriteHeader(http.StatusOK)
	io.Copy(rw, &pv)
}

func (rh *ReaderHandler) HandleCount(rw http.ResponseWriter, r *http.Request) {
	i, err := rh.reader.Count()
	if err != nil {
		readerServiceUnavailable("", err, rw)
		return
	}
	log.Infof("Got a count back of '%v'", i)
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(http.StatusOK)
	b := []byte{}
	b = strconv.AppendInt(b, i, 10)
	rw.Write(b)
}

func (rh *ReaderHandler) HandleGetAll(rw http.ResponseWriter, r *http.Request) {
	pv, err := rh.reader.GetAll()

	if err != nil {
		readerServiceUnavailable(r.URL.RequestURI(), err, rw)
		return
	}

	rw.Header().Set("Content-Type", "application/octet-stream")
	rw.WriteHeader(http.StatusOK)
	io.Copy(rw, &pv)
}

func (rh *ReaderHandler) HandleGet(rw http.ResponseWriter, r *http.Request) {
	uuid := uuid(r.URL.Path)
	publishedDate := r.URL.Query().Get("publishedDate")
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
