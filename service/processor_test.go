package service

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Financial-Times/message-queue-gonsumer/consumer"
	transactionid "github.com/Financial-Times/transactionid-utils-go"
	log "github.com/sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"math/rand"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
)

const (
	expectedUUID          = "123e4567-e89b-12d3-a456-426655440000"
	expectedMessageID     = "7654e321-b98e-3d12-654a-000042665544"
	expectedContentType   = "content/type"
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
	err = w.Write(expectedUUID, "", &p, ct, expectedTransactionId)
	assert.NoError(t, err)
	assert.NotEmpty(t, s.putObjectInput)
	assert.Equal(t, "test/prefix/123e4567/e89b/12d3/a456/426655440000", *s.putObjectInput.Key)
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
	r := newRequest("PUT", "https://url", "Some body")
	r.Header.Set(transactionid.TransactionIDHeader, expectedTransactionId)
	mw := &mockWriter{}
	mr := &mockReader{}
	resWriter := httptest.NewRecorder()
	handler := NewWriterHandler(mw, mr)

	handler.HandleWrite(resWriter, r)

	assert.Equal(t, expectedTransactionId, mw.tid)

	w, s := getWriter()

	err := w.Write(expectedUUID, &[]byte{}, "", mw.tid)

	assert.NoError(t, err)
	assert.Equal(t, expectedTransactionId, *s.putObjectInput.Metadata[transactionid.TransactionIDKey])
}

func TestWritingToS3WithNewTransactionID(t *testing.T) {
	r := newRequest("PUT", "https://url", "Some body")
	mw := &mockWriter{}
	mr := &mockReader{}
	resWriter := httptest.NewRecorder()
	handler := NewWriterHandler(mw, mr)

	handler.HandleWrite(resWriter, r)

	assert.Equal(t, 14, len(mw.tid))

	w, s := getWriter()

	err := w.Write(expectedUUID, &[]byte{}, "", mw.tid)

	assert.NoError(t, err)
	assert.Equal(t, mw.tid, *s.putObjectInput.Metadata[transactionid.TransactionIDKey])
}

func TestWritingToS3WithNoContentType(t *testing.T) {
	w, s := getWriter()
	p := []byte("PAYLOAD")
	var err error
	err = w.Write(expectedUUID, &p, "", expectedTransactionId)
	assert.NoError(t, err)
	assert.NotEmpty(t, s.putObjectInput)
	assert.Equal(t, "test/prefix/123e4567/e89b/12d3/a456/426655440000", *s.putObjectInput.Key)
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
	err := w.Write(expectedUUID,"", &p, ct, expectedTransactionId)
	assert.Error(t, err)
}

func TestGetFromS3(t *testing.T) {
	r, s := getReader()
	s.payload = "PAYLOAD"
	s.ct = expectedContentType
	b, i, ct, err := r.Get(expectedUUID,"")
	assert.NoError(t, err)
	assert.NotEmpty(t, s.getObjectInput)
	assert.Equal(t, "test/prefix/123e4567/e89b/12d3/a456/426655440000", *s.getObjectInput.Key)
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
	b, i, ct, err := r.Get(expectedUUID,"")
	assert.NoError(t, err)
	assert.NotEmpty(t, s.getObjectInput)
	assert.Equal(t, "/123e4567/e89b/12d3/a456/426655440000", *s.getObjectInput.Key)
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
	b, i, ct, err := r.Get(expectedUUID,"")
	assert.NoError(t, err)
	assert.False(t, b)
	assert.Nil(t, i)
	assert.Nil(t, ct)
}

func TestGetFromS3WithUnknownError(t *testing.T) {
	r, s := getReader()
	s.s3error = awserr.New("I don't know", "message", errors.New("Some error"))
	s.payload = "ERROR PAYLOAD"
	b, i, ct, err := r.Get(expectedUUID,"")
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
	b, i, ct, err := r.Get(expectedUUID,"")
	assert.Error(t, err)
	assert.Equal(t, s.s3error, err)
	assert.False(t, b)
	assert.Nil(t, i)
	assert.Nil(t, ct)
}

func TestGetCountFromS3(t *testing.T) {
	r, s := getReader()
	lo1 := generateKeys(100, false)
	lo2 := generateKeys(1, false)
	s.listObjectsV2Outputs = []*s3.ListObjectsV2Output{
		&lo1,
		&lo2,
	}
	i, err := r.Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(101), i)
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
		keys[count+1] = &s3.Object{Key: aws.String(fmt.Sprintf("__gtg %012d/", count+1))}                           // ignored as starts with '__'
		count++
	}

	return s3.ListObjectsV2Output{
		KeyCount: aws.Int64(int64(fc)),
		Contents: keys,
	}
}

func TestGetCountFromS3WithoutPrefix(t *testing.T) {
	r, s := getReaderNoPrefix()
	lo1 := generateKeys(100, false)
	lo2 := generateKeys(1, false)
	s.listObjectsV2Outputs = []*s3.ListObjectsV2Output{
		&lo1,
		&lo2,
	}
	i, err := r.Count()
	assert.NoError(t, err)
	assert.Equal(t, int64(101), i)
	assert.NotEmpty(t, s.listObjectsV2Input)
	assert.Nil(t, s.listObjectsV2Input[0].Prefix)
}

func TestGetCountFromS3Errors(t *testing.T) {
	r, s := getReader()
	s.s3error = errors.New("Some error")
	_, err := r.Count()
	assert.Error(t, err)
	assert.Equal(t, s.s3error, err)
}

func BenchmarkS3Reader_Count(b *testing.B) {
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		r, s := getReaderNoPrefix()
		t := 1000
		for i := 0; i < t; i++ {
			var lo s3.ListObjectsV2Output
			lo = generateKeys(t, true)
			s.listObjectsV2Outputs = append(s.listObjectsV2Outputs, &lo)
		}
		b.StartTimer()
		i, err := r.Count()
		assert.NoError(b, err)
		assert.Equal(b, int64(t*t), i)
	}
}
func BenchmarkS3QProcessor_ProcessMsg(b *testing.B) {
	m := getLargeKMsg()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		mw := &mockWriter{}
		qp := NewQProcessor(mw)

		qp.ProcessMsg(m)

		assert.Equal(b, expectedUUID, mw.uuid)
		assert.Equal(b, expectedTransactionId, mw.tid)
		assert.Equal(b, expectedContentType, mw.ct)

	}
}

func getLargeKMsg() consumer.Message {
	buf := new(bytes.Buffer)
	je := json.NewEncoder(buf)
	bd := make([]byte, 16777216)
	rand.Read(bd)
	c := struct {
		Id   string `json:"uuid"`
		Body []byte `json:"body"`
	}{
		Id:   expectedUUID,
		Body: bd,
	}
	err := je.Encode(&c)
	if err != nil {
		log.Panicf("err encoding, %v", err.Error())
	}

	h := map[string]string{
		"Message-Id":   expectedMessageID,
		"X-Request-Id": expectedTransactionId,
		"Content-Type": expectedContentType,
	}
	return consumer.Message{
		Headers: h,
		Body:    buf.String(),
	}
}

func TestGetIdsFromS3(t *testing.T) {
	r, s := getReader()
	s.listObjectsV2Outputs = []*s3.ListObjectsV2Output{
		{KeyCount: aws.Int64(1)},
		{
			KeyCount: aws.Int64(5),
			Contents: []*s3.Object{
				{Key: aws.String("test/prefix/123e4567/e89b/12d3/a456/426655440001")},
				{Key: aws.String("test/prefix/123e4567/e89b/12d3/a456/426655440002")},
				{Key: aws.String("test/prefix/123e4567/e89b/12d3/a456/426655440003")},
				{Key: aws.String("test/prefix/123e4567/e89b/12d3/a456/426655440004")},
				{Key: aws.String("test/prefix/123e4567/e89b/12d3/a456/426655440005")},
			},
		},
		{
			KeyCount: aws.Int64(5),
			Contents: []*s3.Object{
				{Key: aws.String("test/prefix/123e4567/e89b/12d3/a456/426655440006")},
				{Key: aws.String("test/prefix/123e4567/e89b/12d3/a456/426655440007")},
				{Key: aws.String("test/prefix/123e4567/e89b/12d3/a456/426655440008")},
				{Key: aws.String("test/prefix/123e4567/e89b/12d3/a456/426655440009")},
				{Key: aws.String("test/prefix/123e4567/e89b/12d3/a456/426655440010")},
			},
		},
	}
	p, err := r.Ids()
	assert.NoError(t, err)
	payload, err := ioutil.ReadAll(&p)
	assert.NoError(t, err)
	assert.Equal(t, `{"ID":"123e4567-e89b-12d3-a456-426655440001"}
{"ID":"123e4567-e89b-12d3-a456-426655440002"}
{"ID":"123e4567-e89b-12d3-a456-426655440003"}
{"ID":"123e4567-e89b-12d3-a456-426655440004"}
{"ID":"123e4567-e89b-12d3-a456-426655440005"}
{"ID":"123e4567-e89b-12d3-a456-426655440006"}
{"ID":"123e4567-e89b-12d3-a456-426655440007"}
{"ID":"123e4567-e89b-12d3-a456-426655440008"}
{"ID":"123e4567-e89b-12d3-a456-426655440009"}
{"ID":"123e4567-e89b-12d3-a456-426655440010"}
`, string(payload[:]))
}

func TestGetIdsFromS3NoPrefix(t *testing.T) {
	r, s := getReaderNoPrefix()
	s.listObjectsV2Outputs = []*s3.ListObjectsV2Output{
		{KeyCount: aws.Int64(1)},
		{
			KeyCount: aws.Int64(5),
			Contents: []*s3.Object{
				{Key: aws.String("123e4567/e89b/12d3/a456/426655440001")},
				{Key: aws.String("123e4567/e89b/12d3/a456/426655440002")},
				{Key: aws.String("123e4567/e89b/12d3/a456/426655440003")},
				{Key: aws.String("123e4567/e89b/12d3/a456/426655440004")},
				{Key: aws.String("123e4567/e89b/12d3/a456/426655440005")},
			},
		},
		{
			KeyCount: aws.Int64(5),
			Contents: []*s3.Object{
				{Key: aws.String("123e4567/e89b/12d3/a456/426655440006")},
				{Key: aws.String("123e4567/e89b/12d3/a456/426655440007")},
				{Key: aws.String("123e4567/e89b/12d3/a456/426655440008")},
				{Key: aws.String("123e4567/e89b/12d3/a456/426655440009")},
				{Key: aws.String("123e4567/e89b/12d3/a456/426655440010")},
			},
		},
	}
	p, err := r.Ids()
	assert.NoError(t, err)
	payload, err := ioutil.ReadAll(&p)
	assert.NoError(t, err)
	assert.Equal(t, `{"ID":"123e4567-e89b-12d3-a456-426655440001"}
{"ID":"123e4567-e89b-12d3-a456-426655440002"}
{"ID":"123e4567-e89b-12d3-a456-426655440003"}
{"ID":"123e4567-e89b-12d3-a456-426655440004"}
{"ID":"123e4567-e89b-12d3-a456-426655440005"}
{"ID":"123e4567-e89b-12d3-a456-426655440006"}
{"ID":"123e4567-e89b-12d3-a456-426655440007"}
{"ID":"123e4567-e89b-12d3-a456-426655440008"}
{"ID":"123e4567-e89b-12d3-a456-426655440009"}
{"ID":"123e4567-e89b-12d3-a456-426655440010"}
`, string(payload[:]))
}

func TestGetIdsFromS3Fails(t *testing.T) {
	r, s := getReader()
	s.s3error = errors.New("Some error")
	_, err := r.Ids()
	assert.Error(t, err)
	assert.Equal(t, s.s3error, err)
}

func TestReaderHandler_HandleGetAllOK(t *testing.T) {
	r, s := getReader()
	s.payload = "PAYLOAD"

	s.listObjectsV2Outputs = []*s3.ListObjectsV2Output{
		{KeyCount: aws.Int64(1)},
		{
			KeyCount: aws.Int64(5),
			Contents: []*s3.Object{
				{Key: aws.String("test/prefix/UUID-1")},
				{Key: aws.String("test/prefix/UUID-2")},
				{Key: aws.String("test/prefix/UUID-3")},
				{Key: aws.String("test/prefix/UUID-4")},
				{Key: aws.String("__gtg")}, // ignored as starts with '__'
				{Key: aws.String("test/prefix/UUID-5")},
			},
		},
		{
			KeyCount: aws.Int64(5),
			Contents: []*s3.Object{
				{Key: aws.String("test/prefix/UUID-6")},
				{Key: aws.String("test/prefix/UUID-7")},
				{Key: aws.String("test/prefix/UUID-8")},
				{Key: aws.String("test/prefix/UUID-9")},
				{Key: aws.String("test/prefix/folder/")}, // ignored as ends with '/'
				{Key: aws.String("test/prefix/UUID-10")},
			},
		},
	}
	p, err := r.GetAll()
	assert.NoError(t, err)
	payload, err := ioutil.ReadAll(&p)
	assert.NoError(t, err)
	assert.Equal(t, `PAYLOAD0
PAYLOAD1
PAYLOAD2
PAYLOAD3
PAYLOAD4
PAYLOAD5
PAYLOAD6
PAYLOAD7
PAYLOAD8
PAYLOAD9
`, string(payload[:]))

}

func TestReaderHandler_HandleGetAllOKWithLotsOfWorkers(t *testing.T) {
	r, s := getReaderWithMultipleWorkers()
	s.payload = "PAYLOAD"
	s.listObjectsV2Outputs = []*s3.ListObjectsV2Output{
		{KeyCount: aws.Int64(1)},
		getListObjectsV2Output(5, 0),
		getListObjectsV2Output(5, 5),
		getListObjectsV2Output(5, 15),
		getListObjectsV2Output(5, 20),
		getListObjectsV2Output(5, 25),
	}
	p, err := r.GetAll()
	assert.NoError(t, err)
	payload, err := ioutil.ReadAll(&p)
	assert.NoError(t, err)
	assert.Equal(t, 25, strings.Count(string(payload[:]), "PAYLOAD"))
}

func TestS3Reader_Head(t *testing.T) {
	r, s := getReader()
	t.Run("Found", func(t *testing.T) {
		found, err := r.Head(expectedUUID, "")
		assert.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, "test/prefix/123e4567/e89b/12d3/a456/426655440000", *s.headObjectInput.Key)
		assert.Equal(t, "testBucket", *s.headObjectInput.Bucket)

	})

	t.Run("NotFound", func(t *testing.T) {
		s.s3error = awserr.New("NotFound", "message", errors.New("Some error"))
		found, err := r.Head(expectedUUID, "")
		assert.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("Random Error", func(t *testing.T) {
		s.s3error = errors.New("Random error")
		found, err := r.Head(expectedUUID, "")
		assert.Error(t, err)
		assert.False(t, found)
		assert.Equal(t, s.s3error, err)
	})
}

func getListObjectsV2Output(keyCount int64, start int) *s3.ListObjectsV2Output {
	contents := []*s3.Object{}
	for i := start; i < start+int(keyCount); i++ {
		contents = append(contents, &s3.Object{Key: aws.String("test/prefix/UUID-" + strconv.Itoa(i))})
	}
	return &s3.ListObjectsV2Output{
		KeyCount: aws.Int64(keyCount),
		Contents: contents,
	}
}

func TestS3Reader_GetAllFails(t *testing.T) {
	r, s := getReader()
	s.s3error = errors.New("Some error")
	_, err := r.GetAll()
	assert.Error(t, err)
	assert.Equal(t, s.s3error, err)
}

func TestDelete(t *testing.T) {
	var w Writer
	var s *mockS3Client

	t.Run("With prefix", func(t *testing.T) {
		w, s = getWriter()
		err := w.Delete(expectedUUID, "")
		assert.NoError(t, err)
		assert.Equal(t, "test/prefix/123e4567/e89b/12d3/a456/426655440000", *s.deleteObjectInput.Key)
		assert.Equal(t, "testBucket", *s.deleteObjectInput.Bucket)
	})

	t.Run("Without prefix", func(t *testing.T) {
		w, s = getWriterNoPrefix()
		err := w.Delete(expectedUUID, "")
		assert.NoError(t, err)
		assert.Equal(t, "/123e4567/e89b/12d3/a456/426655440000", *s.deleteObjectInput.Key)
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

func TestS3QProcessor_ProcessMsgCorrectly(t *testing.T) {
	testCases := []struct {
		uuid  string
		ct    string
		wUuid string
		wCt   string
		tid   string
	}{
		{expectedUUID, expectedContentType, expectedUUID, expectedContentType, expectedTransactionId},
		{expectedUUID, "", expectedUUID, "", expectedTransactionId},
		{"", expectedContentType, expectedMessageID, expectedContentType, expectedTransactionId},
		{expectedUUID, expectedContentType, expectedUUID, expectedContentType, ""},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Msg with [uuid=%v, ct=%v], expect [uuid=%v, ct=%v]", tc.uuid, tc.ct, tc.wUuid, tc.wCt), func(t *testing.T) {
			mw := &mockWriter{}
			qp := NewQProcessor(mw)
			m := generateConsumerMessage(tc.ct, tc.uuid)

			qp.ProcessMsg(m)

			assert.Equal(t, tc.wUuid, mw.uuid)
			assert.Equal(t, tc.wCt, mw.ct)
			assert.Equal(t, m.Body, mw.payload)
			assert.NotEmpty(t, mw.tid)
		})
	}
}

func TestS3QProcessor_ProcessMsgNoneJsonShouldNotCallWriter(t *testing.T) {
	mw := &mockWriter{}
	qp := NewQProcessor(mw)
	m := generateConsumerMessage(expectedContentType, expectedUUID)
	m.Body = "none json data is here"
	qp.ProcessMsg(m)
	assert.False(t, mw.writeCalled)
}

func TestS3QProcessor_ProcessMsgDealsWithErrorsFromWriter(t *testing.T) {
	mw := &mockWriter{}
	mw.returnError = errors.New("Some error")
	qp := NewQProcessor(mw)
	m := generateConsumerMessage(expectedContentType, expectedUUID)
	qp.ProcessMsg(m)
	assert.True(t, mw.writeCalled)
	assert.Equal(t, expectedUUID, mw.uuid)
	assert.Equal(t, expectedTransactionId, mw.tid)
	assert.Equal(t, expectedContentType, mw.ct)

	assert.Equal(t, m.Body, mw.payload)
}

func generateConsumerMessage(ct string, cid string) consumer.Message {
	h := map[string]string{
		"Message-Id":   expectedMessageID,
		"X-Request-Id": expectedTransactionId,
	}
	if ct != "" {
		h["Content-Type"] = ct
	}

	return consumer.Message{
		Body: fmt.Sprintf(`{
		"uuid": "%v",
		"random":"one",
		"data": "e1wic29tZVwiOlwiZGF0YVwifQ=="
		}`, cid),
		Headers: h,
	}
}

func getReader() (Reader, *mockS3Client) {
	s := &mockS3Client{}
	return NewS3Reader(s, "testBucket", "test/prefix", 1), s
}

func getReaderWithMultipleWorkers() (Reader, *mockS3Client) {
	s := &mockS3Client{}
	return NewS3Reader(s, "testBucket", "test/prefix", 15), s
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
