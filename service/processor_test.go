package service

import (
	"errors"
	"fmt"
	transactionid "github.com/Financial-Times/transactionid-utils-go"
	log "github.com/sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
)

const (
	expectedUUID = "123e4567-e89b-12d3-a456-426655440000"
	expectedMessageID = "7654e321-b98e-3d12-654a-000042665544"
	expectedContentType = "content/type"
	expectedTransactionId = "tid_0123456789"
)

type mockS3Client struct {
	s3iface.S3API
	sync.Mutex
	s3error              error
	putObjectInput       *s3.PutObjectInput
	headBucketInput      *s3.HeadBucketInput
	headObjectInput      *s3.HeadObjectInput
	getObjectInput       *s3.GetObjectInput
	deleteObjectInput    *s3.DeleteObjectInput
	deleteObjectOutput   *s3.DeleteObjectOutput
	listObjectsV2Outputs []*s3.ListObjectsV2Output
	listObjectsV2Input   []*s3.ListObjectsV2Input
	count                int
	getObjectCount       int
	payload              string
	ct                   string
}

func (m *mockS3Client) PutObject(poi *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	m.Lock()
	defer m.Unlock()
	log.Infof("Put params: %v", poi)
	m.putObjectInput = poi
	return nil, m.s3error
}

func (m *mockS3Client) HeadBucket(hbi *s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
	m.Lock()
	defer m.Unlock()
	log.Infof("Head params: %v", hbi)
	m.headBucketInput = hbi
	return nil, m.s3error
}
func (m *mockS3Client) HeadObject(hoi *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	m.Lock()
	defer m.Unlock()
	log.Infof("Head params: %v", hoi)
	m.headObjectInput = hoi
	return nil, m.s3error

}
func (m *mockS3Client) DeleteObject(doi *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	m.Lock()
	defer m.Unlock()
	log.Infof("Delete params: %v", doi)
	m.deleteObjectInput = doi
	return m.deleteObjectOutput, m.s3error
}

func (m *mockS3Client) GetObject(goi *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	m.Lock()
	defer m.Unlock()
	log.Infof("Get params: %v", goi)
	m.getObjectInput = goi
	payload := m.payload + strconv.Itoa(m.getObjectCount)
	m.getObjectCount++
	return &s3.GetObjectOutput{
		Body:        ioutil.NopCloser(strings.NewReader(payload)),
		ContentType: aws.String(m.ct),
	}, m.s3error
}

func (m *mockS3Client) ListObjectsV2(loi *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	m.Lock()
	defer m.Unlock()
	log.Infof("Get ListObjectsV2: %v", loi)
	m.listObjectsV2Input = append(m.listObjectsV2Input, loi)
	var loo *s3.ListObjectsV2Output
	if len(m.listObjectsV2Outputs) > 0 {
		loo = m.listObjectsV2Outputs[m.count]
	}
	m.count++
	return loo, m.s3error
}
func (m *mockS3Client) ListObjectsV2Pages(loi *s3.ListObjectsV2Input, fn func(p *s3.ListObjectsV2Output, lastPage bool) (shouldContinue bool)) error {
	m.Lock()
	defer m.Unlock()
	log.Debugf("Get ListObjectsV2Pages: %v", loi)
	m.listObjectsV2Input = append(m.listObjectsV2Input, loi)

	for i := m.count; i < len(m.listObjectsV2Outputs); i++ {
		lastPage := i == (len(m.listObjectsV2Outputs) - 1)
		fn(m.listObjectsV2Outputs[i], lastPage)
	}

	return m.s3error
}

func TestWritingToS3(t *testing.T) {
	w, s := getWriter()
	p := []byte("PAYLOAD")
	ct := expectedContentType
	var err error
	err = w.Write(expectedUUID, "2017-10-10", &p, ct, expectedTransactionId)
	assert.NoError(t, err)
	assert.NotEmpty(t, s.putObjectInput)
	assert.Equal(t, "test/prefix/123e4567-e89b-12d3-a456-426655440000_2017-10-10.json", *s.putObjectInput.Key)
	assert.Equal(t, "testBucket", *s.putObjectInput.Bucket)
	assert.Equal(t, expectedContentType, *s.putObjectInput.ContentType)

	rs := s.putObjectInput.Body
	assert.NotNil(t, rs)
	ba, err := ioutil.ReadAll(rs)
	assert.NoError(t, err)
	body := string(ba[:])
	assert.Equal(t, "PAYLOAD", body)
}

func TestWritingToS3WithTransactionID(t *testing.T) {
	r := newRequest("PUT", "https://url?date=2016-10-10", "Some body")
	r.Header.Set(transactionid.TransactionIDHeader, expectedTransactionId)
	mw := &mockWriter{}
	mr := &mockReader{}
	resWriter := httptest.NewRecorder()
	handler := NewWriterHandler(mw, mr)

	handler.HandleWrite(resWriter, r)

	assert.Equal(t, expectedTransactionId, mw.tid)

	w, s := getWriter()

	err := w.Write(expectedUUID, "", &[]byte{}, "", mw.tid)

	assert.NoError(t, err)
	assert.Equal(t, expectedTransactionId, *s.putObjectInput.Metadata[transactionid.TransactionIDKey])
}

func TestWritingToS3WithNewTransactionID(t *testing.T) {
	r := newRequest("PUT", "https://url?date=2017-10-10", "Some body")
	mw := &mockWriter{}
	mr := &mockReader{}
	resWriter := httptest.NewRecorder()
	handler := NewWriterHandler(mw, mr)

	handler.HandleWrite(resWriter, r)

	assert.Equal(t, 14, len(mw.tid))

	w, s := getWriter()

	err := w.Write(expectedUUID, "", &[]byte{}, "", mw.tid)

	assert.NoError(t, err)
	assert.Equal(t, mw.tid, *s.putObjectInput.Metadata[transactionid.TransactionIDKey])
}

func TestWritingToS3WithNoContentType(t *testing.T) {
	w, s := getWriter()
	p := []byte("PAYLOAD")
	var err error
	err = w.Write(expectedUUID, "2017-10-10", &p, "", expectedTransactionId)
	assert.NoError(t, err)
	assert.NotEmpty(t, s.putObjectInput)
	assert.Equal(t, "test/prefix/123e4567-e89b-12d3-a456-426655440000_2017-10-10.json", *s.putObjectInput.Key)
	assert.Equal(t, "testBucket", *s.putObjectInput.Bucket)
	assert.Nil(t, s.putObjectInput.ContentType)

	rs := s.putObjectInput.Body
	assert.NotNil(t, rs)
	ba, err := ioutil.ReadAll(rs)
	assert.NoError(t, err)
	body := string(ba[:])
	assert.Equal(t, "PAYLOAD", body)
}

func TestFailingToWriteToS3(t *testing.T) {
	w, s := getWriter()
	p := []byte("PAYLOAD")
	ct := expectedContentType
	s.s3error = errors.New("S3 error")
	err := w.Write(expectedUUID, "", &p, ct, expectedTransactionId)
	assert.Error(t, err)
}

func TestGetFromS3(t *testing.T) {
	r, s := getReader()
	s.payload = "PAYLOAD"
	s.ct = expectedContentType
	b, i, ct, err := r.Get(expectedUUID, "2016-10-10")
	assert.NoError(t, err)
	assert.NotEmpty(t, s.getObjectInput)
	assert.Equal(t, "test/prefix/123e4567-e89b-12d3-a456-426655440000_2016-10-10.json", *s.getObjectInput.Key)
	assert.Equal(t, "testBucket", *s.getObjectInput.Bucket)
	assert.Equal(t, expectedContentType, *ct)
	assert.True(t, b)
	p, _ := ioutil.ReadAll(i)
	assert.Equal(t, "PAYLOAD0", string(p[:]))
}
func TestGetFromS3NoPrefix(t *testing.T) {
	r, s := getReaderNoPrefix()
	s.payload = "PAYLOAD"
	s.ct = expectedContentType
	b, i, ct, err := r.Get(expectedUUID, "2016-10-10")
	assert.NoError(t, err)
	assert.NotEmpty(t, s.getObjectInput)
	assert.Equal(t, "/123e4567-e89b-12d3-a456-426655440000_2016-10-10.json", *s.getObjectInput.Key)
	assert.Equal(t, "testBucket", *s.getObjectInput.Bucket)
	assert.Equal(t, expectedContentType, *ct)
	assert.True(t, b)
	p, _ := ioutil.ReadAll(i)
	assert.Equal(t, "PAYLOAD0", string(p[:]))
}

func TestGetFromS3WhenNoSuchKey(t *testing.T) {
	r, s := getReader()
	s.s3error = awserr.New("NoSuchKey", "message", errors.New("Some error"))
	s.payload = "PAYLOAD"
	b, i, ct, err := r.Get(expectedUUID, "")
	assert.NoError(t, err)
	assert.False(t, b)
	assert.Nil(t, i)
	assert.Nil(t, ct)
}

func TestGetFromS3WithUnknownError(t *testing.T) {
	r, s := getReader()
	s.s3error = awserr.New("I don't know", "message", errors.New("Some error"))
	s.payload = "ERROR PAYLOAD"
	b, i, ct, err := r.Get(expectedUUID, "")
	assert.Error(t, err)
	assert.Equal(t, s.s3error, err)
	assert.False(t, b)
	assert.Nil(t, i)
	assert.Nil(t, ct)
}

func TestGetFromS3WithNoneAWSError(t *testing.T) {
	r, s := getReader()
	s.s3error = errors.New("Some error")
	s.payload = "ERROR PAYLOAD"
	b, i, ct, err := r.Get(expectedUUID, "")
	assert.Error(t, err)
	assert.Equal(t, s.s3error, err)
	assert.False(t, b)
	assert.Nil(t, i)
	assert.Nil(t, ct)
}

func generateKeys(count int, addIgnoredKeys bool) s3.ListObjectsV2Output {
	fc := count
	if addIgnoredKeys {
		fc = fc + 2
	}
	keys := make([]*s3.Object, fc)
	for i := 0; i < count; i++ {
		keys[i] = &s3.Object{Key: aws.String(fmt.Sprintf("test/prefix/123e4567/e89b/12d3/a456/%012d", i))}
	}

	if addIgnoredKeys {
		keys[count] = &s3.Object{Key: aws.String(fmt.Sprintf("test/prefix/123e4567/e89b/12d3/a456/%012d/", count))} // ignored as ends with '/'
		keys[count + 1] = &s3.Object{Key: aws.String(fmt.Sprintf("__gtg %012d/", count + 1))}                           // ignored as starts with '__'
		count++
	}

	return s3.ListObjectsV2Output{
		KeyCount: aws.Int64(int64(fc)),
		Contents: keys,
	}
}

func TestDelete(t *testing.T) {
	var w Writer
	var s *mockS3Client

	t.Run("With prefix", func(t *testing.T) {
		w, s = getWriter()
		err := w.Delete(expectedUUID, "2017-01-06")
		assert.NoError(t, err)
		assert.Equal(t, "test/prefix/123e4567-e89b-12d3-a456-426655440000_2017-01-06.json", *s.deleteObjectInput.Key)
		assert.Equal(t, "testBucket", *s.deleteObjectInput.Bucket)
	})

	t.Run("Without prefix", func(t *testing.T) {
		w, s = getWriterNoPrefix()
		err := w.Delete(expectedUUID, "2017-01-06")
		assert.NoError(t, err)
		assert.Equal(t, "/123e4567-e89b-12d3-a456-426655440000_2017-01-06.json", *s.deleteObjectInput.Key)
		assert.Equal(t, "testBucket", *s.deleteObjectInput.Bucket)
	})

	t.Run("Fails", func(t *testing.T) {
		w, s = getWriter()
		s.s3error = errors.New("Some S3 error")
		err := w.Delete(expectedUUID, "")
		assert.Error(t, err)
		assert.Equal(t, s.s3error, err)
	})
}

func getReader() (Reader, *mockS3Client) {
	s := &mockS3Client{}
	return NewS3Reader(s, "testBucket", "test/prefix", 1), s
}

func getReaderNoPrefix() (Reader, *mockS3Client) {
	s := &mockS3Client{}
	return NewS3Reader(s, "testBucket", "", 1), s
}

func getWriter() (Writer, *mockS3Client) {
	s := &mockS3Client{}
	return NewS3Writer(s, "testBucket", "test/prefix"), s
}

func getWriterNoPrefix() (Writer, *mockS3Client) {
	s := &mockS3Client{}
	return NewS3Writer(s, "testBucket", ""), s
}
