package s3

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/superfly/ltx"
)

const manifestTestBucket = "test-bucket"

var manifestTestLastModified = time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)

type manifestTestStore struct {
	t              *testing.T
	mu             sync.Mutex
	objects        map[string][]byte
	failures       map[string][]error
	deleteFailures [][]manifestTestDeleteError
	operations     map[string]int
}

func newManifestTestStore(t *testing.T) *manifestTestStore {
	t.Helper()
	return &manifestTestStore{
		t:          t,
		objects:    make(map[string][]byte),
		failures:   make(map[string][]error),
		operations: make(map[string]int),
	}
}

func (s *manifestTestStore) newClient(path string) *ReplicaClient {
	s.t.Helper()

	httpClient := smithyhttp.ClientDoFunc(s.do)
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(DefaultRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test-access-key", "test-secret-key", "")),
		config.WithHTTPClient(httpClient),
		config.WithRetryer(func() aws.Retryer {
			return retry.NewStandard(func(options *retry.StandardOptions) {
				options.MaxAttempts = 1
			})
		}),
	)
	if err != nil {
		s.t.Fatalf("load AWS config: %v", err)
	}

	client := NewReplicaClient()
	client.Bucket = manifestTestBucket
	client.Path = path
	client.Region = DefaultRegion
	client.Endpoint = "https://s3.test"
	client.ForcePathStyle = true
	client.AccessKeyID = "test-access-key"
	client.SecretAccessKey = "test-secret-key"
	client.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	client.s3 = awss3.NewFromConfig(cfg, func(options *awss3.Options) {
		options.BaseEndpoint = aws.String(client.Endpoint)
		options.UsePathStyle = true
		options.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		options.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		options.APIOptions = append(options.APIOptions, client.middlewareOption())
	})
	client.uploader = manager.NewUploader(client.s3, func(uploader *manager.Uploader) {
		uploader.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	})
	return client
}

func (s *manifestTestStore) failNext(operation string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failures[operation] = append(s.failures[operation], err)
}

func (s *manifestTestStore) failNextDeleteObjects(failures ...manifestTestDeleteError) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleteFailures = append(s.deleteFailures, failures)
}

func (s *manifestTestStore) operationCount(operation string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.operations[operation]
}

func (s *manifestTestStore) putObject(key string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = bytes.Clone(data)
}

func (s *manifestTestStore) object(t *testing.T, key string) []byte {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.objects[key]
	if !ok {
		t.Fatalf("object not found: %s", key)
	}
	return bytes.Clone(data)
}

func (s *manifestTestStore) hasObject(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.objects[key]
	return ok
}

func (s *manifestTestStore) putLTX(t *testing.T, path string, level int, minTXID, maxTXID ltx.TXID) {
	t.Helper()
	key := fmt.Sprintf("%s/%04x/%s", path, level, ltx.FormatFilename(minTXID, maxTXID))
	s.putObject(key, mustManifestTestLTX(t, minTXID, maxTXID))
}

func (s *manifestTestStore) hasLTX(path string, level int, minTXID, maxTXID ltx.TXID) bool {
	key := fmt.Sprintf("%s/%04x/%s", path, level, ltx.FormatFilename(minTXID, maxTXID))
	return s.hasObject(key)
}

func (s *manifestTestStore) putManifest(t *testing.T, path string, manifest *Manifest) {
	t.Helper()
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("encode manifest: %v", err)
	}
	s.putObject(path+"/manifest.json", data)
}

func (s *manifestTestStore) manifest(t *testing.T, path string) *Manifest {
	t.Helper()
	data := s.object(t, path+"/manifest.json")
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	return &manifest
}

func (s *manifestTestStore) do(r *http.Request) (*http.Response, error) {
	operation := manifestTestOperation(r)
	if operation == "" {
		return nil, fmt.Errorf("unsupported S3 request: %s %s", r.Method, r.URL.String())
	}

	s.mu.Lock()
	s.operations[operation]++
	var failure error
	if failures := s.failures[operation]; len(failures) != 0 {
		failure = failures[0]
		s.failures[operation] = failures[1:]
	}
	s.mu.Unlock()
	if failure != nil {
		return nil, failure
	}

	switch operation {
	case "GetObject":
		return s.getObjectResponse(r)
	case "PutObject":
		return s.putObjectResponse(r)
	case "DeleteObject":
		return s.deleteObjectResponse(r)
	case "DeleteObjects":
		return s.deleteObjectsResponse(r)
	case "ListObjectsV2":
		return s.listObjectsV2Response(r)
	default:
		return nil, fmt.Errorf("unsupported S3 operation: %s", operation)
	}
}

func manifestTestOperation(r *http.Request) string {
	query := r.URL.Query()
	switch {
	case r.Method == http.MethodGet && query.Get("list-type") == "2":
		return "ListObjectsV2"
	case r.Method == http.MethodGet:
		return "GetObject"
	case r.Method == http.MethodPut:
		return "PutObject"
	case r.Method == http.MethodDelete:
		return "DeleteObject"
	case r.Method == http.MethodPost && query.Has("delete"):
		return "DeleteObjects"
	default:
		return ""
	}
}

func (s *manifestTestStore) getObjectResponse(r *http.Request) (*http.Response, error) {
	key, err := manifestTestObjectKey(r.URL)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	data, ok := s.objects[key]
	data = bytes.Clone(data)
	s.mu.Unlock()
	if !ok {
		return manifestTestErrorResponse(http.StatusNotFound, "NoSuchKey", "The specified key does not exist", key), nil
	}
	totalSize := len(data)
	etag := manifestTestETag(data)

	statusCode := http.StatusOK
	start, end := int64(0), int64(totalSize-1)
	if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
		start, end, err = manifestTestRange(rangeHeader, int64(len(data)))
		if err != nil {
			return manifestTestErrorResponse(http.StatusRequestedRangeNotSatisfiable, "InvalidRange", err.Error(), key), nil
		}
		statusCode = http.StatusPartialContent
		data = data[start : end+1]
	}

	header := http.Header{
		"Accept-Ranges":  []string{"bytes"},
		"Content-Length": []string{strconv.Itoa(len(data))},
		"Content-Type":   []string{"application/octet-stream"},
		"ETag":           []string{etag},
		"Last-Modified":  []string{manifestTestLastModified.Format(http.TimeFormat)},
	}
	if statusCode == http.StatusPartialContent {
		header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, totalSize))
	}
	return manifestTestResponse(statusCode, header, data), nil
}

func (s *manifestTestStore) putObjectResponse(r *http.Request) (*http.Response, error) {
	key, err := manifestTestObjectKey(r.URL)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("read PutObject body: %w", err)
	}
	if err := r.Body.Close(); err != nil {
		return nil, fmt.Errorf("close PutObject body: %w", err)
	}

	s.putObject(key, data)
	return manifestTestResponse(http.StatusOK, http.Header{"ETag": []string{manifestTestETag(data)}}, nil), nil
}

func (s *manifestTestStore) deleteObjectResponse(r *http.Request) (*http.Response, error) {
	key, err := manifestTestObjectKey(r.URL)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	delete(s.objects, key)
	s.mu.Unlock()
	return manifestTestResponse(http.StatusNoContent, nil, nil), nil
}

type manifestTestDeleteRequest struct {
	Objects []manifestTestDeleteObject `xml:"Object"`
}

type manifestTestDeleteObject struct {
	Key string `xml:"Key"`
}

type manifestTestDeleteError struct {
	Key     string `xml:"Key"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

type manifestTestDeleteResult struct {
	XMLName xml.Name                   `xml:"http://s3.amazonaws.com/doc/2006-03-01/ DeleteResult"`
	Deleted []manifestTestDeleteObject `xml:"Deleted"`
	Errors  []manifestTestDeleteError  `xml:"Error"`
}

func (s *manifestTestStore) deleteObjectsResponse(r *http.Request) (*http.Response, error) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("read DeleteObjects body: %w", err)
	}
	if err := r.Body.Close(); err != nil {
		return nil, fmt.Errorf("close DeleteObjects body: %w", err)
	}

	var request manifestTestDeleteRequest
	if err := xml.Unmarshal(data, &request); err != nil {
		return nil, fmt.Errorf("decode DeleteObjects body: %w", err)
	}

	s.mu.Lock()
	var failures []manifestTestDeleteError
	if len(s.deleteFailures) != 0 {
		failures = s.deleteFailures[0]
		s.deleteFailures = s.deleteFailures[1:]
	}
	failureByKey := make(map[string]manifestTestDeleteError, len(failures))
	for _, failure := range failures {
		failureByKey[failure.Key] = failure
	}
	result := manifestTestDeleteResult{}
	for _, object := range request.Objects {
		if failure, ok := failureByKey[object.Key]; ok {
			result.Errors = append(result.Errors, failure)
			continue
		}
		delete(s.objects, object.Key)
		result.Deleted = append(result.Deleted, object)
	}
	s.mu.Unlock()

	return manifestTestXMLResponse(http.StatusOK, result)
}

type manifestTestListResult struct {
	XMLName               xml.Name                 `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult"`
	Name                  string                   `xml:"Name"`
	Prefix                string                   `xml:"Prefix"`
	ContinuationToken     string                   `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string                   `xml:"NextContinuationToken,omitempty"`
	KeyCount              int                      `xml:"KeyCount"`
	MaxKeys               int                      `xml:"MaxKeys"`
	IsTruncated           bool                     `xml:"IsTruncated"`
	Contents              []manifestTestListObject `xml:"Contents"`
}

type manifestTestListObject struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int    `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

func (s *manifestTestStore) listObjectsV2Response(r *http.Request) (*http.Response, error) {
	query := r.URL.Query()
	prefix := query.Get("prefix")
	maxKeys := MaxKeys
	if value := query.Get("max-keys"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 0 {
			return nil, fmt.Errorf("invalid max-keys: %q", value)
		}
		maxKeys = max(parsed, 1)
	}

	start := 0
	if token := query.Get("continuation-token"); token != "" {
		parsed, err := strconv.Atoi(token)
		if err != nil || parsed < 0 {
			return nil, fmt.Errorf("invalid continuation-token: %q", token)
		}
		start = parsed
	}

	s.mu.Lock()
	keys := make([]string, 0, len(s.objects))
	for key := range s.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	objects := make(map[string][]byte, len(keys))
	for _, key := range keys {
		objects[key] = bytes.Clone(s.objects[key])
	}
	s.mu.Unlock()

	if start > len(keys) {
		start = len(keys)
	}
	end := start + maxKeys
	if end > len(keys) {
		end = len(keys)
	}

	result := manifestTestListResult{
		Name:              manifestTestBucket,
		Prefix:            prefix,
		ContinuationToken: query.Get("continuation-token"),
		KeyCount:          end - start,
		MaxKeys:           maxKeys,
		IsTruncated:       end < len(keys),
		Contents:          make([]manifestTestListObject, 0, end-start),
	}
	if result.IsTruncated {
		result.NextContinuationToken = strconv.Itoa(end)
	}
	for _, key := range keys[start:end] {
		result.Contents = append(result.Contents, manifestTestListObject{
			Key:          key,
			LastModified: manifestTestLastModified.Format(time.RFC3339),
			ETag:         manifestTestETag(objects[key]),
			Size:         len(objects[key]),
			StorageClass: "STANDARD",
		})
	}
	return manifestTestXMLResponse(http.StatusOK, result)
}

func manifestTestObjectKey(u *url.URL) (string, error) {
	path, err := url.PathUnescape(strings.TrimPrefix(u.EscapedPath(), "/"))
	if err != nil {
		return "", fmt.Errorf("decode object path: %w", err)
	}
	prefix := manifestTestBucket + "/"
	if !strings.HasPrefix(path, prefix) {
		return "", fmt.Errorf("unexpected bucket path: %s", path)
	}
	key := strings.TrimPrefix(path, prefix)
	if key == "" {
		return "", fmt.Errorf("object key required")
	}
	return key, nil
}

func manifestTestRange(header string, size int64) (start, end int64, err error) {
	if !strings.HasPrefix(header, "bytes=") {
		return 0, 0, fmt.Errorf("invalid range: %s", header)
	}
	parts := strings.Split(strings.TrimPrefix(header, "bytes="), "-")
	if len(parts) != 2 || parts[0] == "" {
		return 0, 0, fmt.Errorf("invalid range: %s", header)
	}
	start, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil || start < 0 || start >= size {
		return 0, 0, fmt.Errorf("invalid range: %s", header)
	}
	end = size - 1
	if parts[1] != "" {
		end, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil || end < start {
			return 0, 0, fmt.Errorf("invalid range: %s", header)
		}
		if end >= size {
			end = size - 1
		}
	}
	return start, end, nil
}

func manifestTestETag(data []byte) string {
	sum := md5.Sum(data)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

func manifestTestXMLResponse(statusCode int, value any) (*http.Response, error) {
	data, err := xml.Marshal(value)
	if err != nil {
		return nil, err
	}
	return manifestTestResponse(statusCode, http.Header{"Content-Type": []string{"application/xml"}}, data), nil
}

func manifestTestErrorResponse(statusCode int, code, message, key string) *http.Response {
	value := struct {
		XMLName   xml.Name `xml:"Error"`
		Code      string   `xml:"Code"`
		Message   string   `xml:"Message"`
		Key       string   `xml:"Key,omitempty"`
		RequestID string   `xml:"RequestId"`
	}{
		Code:      code,
		Message:   message,
		Key:       key,
		RequestID: "manifest-test-request",
	}
	data, err := xml.Marshal(value)
	if err != nil {
		panic(err)
	}
	return manifestTestResponse(statusCode, http.Header{"Content-Type": []string{"application/xml"}}, data)
}

func manifestTestResponse(statusCode int, header http.Header, data []byte) *http.Response {
	normalizedHeader := make(http.Header, len(header)+1)
	for name, values := range header {
		for _, value := range values {
			normalizedHeader.Add(name, value)
		}
	}
	normalizedHeader.Set("Content-Length", strconv.Itoa(len(data)))
	return &http.Response{
		StatusCode:    statusCode,
		Status:        fmt.Sprintf("%d %s", statusCode, http.StatusText(statusCode)),
		Header:        normalizedHeader,
		Body:          io.NopCloser(bytes.NewReader(data)),
		ContentLength: int64(len(data)),
	}
}

func mustManifestTestLTX(t *testing.T, minTXID, maxTXID ltx.TXID) []byte {
	t.Helper()
	preApplyChecksum := ltx.ChecksumFlag | 1
	if minTXID == 1 {
		preApplyChecksum = 0
	}
	buf := new(bytes.Buffer)
	encoder, err := ltx.NewEncoder(buf)
	if err != nil {
		t.Fatalf("new LTX encoder: %v", err)
	}
	if err := encoder.EncodeHeader(ltx.Header{
		Version:          ltx.Version,
		PageSize:         4096,
		MinTXID:          minTXID,
		MaxTXID:          maxTXID,
		Timestamp:        manifestTestLastModified.UnixMilli(),
		PreApplyChecksum: preApplyChecksum,
	}); err != nil {
		t.Fatalf("encode LTX header: %v", err)
	}
	encoder.SetPostApplyChecksum(ltx.ChecksumFlag)
	if err := encoder.Close(); err != nil {
		t.Fatalf("close LTX encoder: %v", err)
	}
	return buf.Bytes()
}

func manifestWithFile(level int, minTXID, maxTXID ltx.TXID) *Manifest {
	manifest := NewManifest()
	manifest.AddFile(&ltx.FileInfo{
		Level:     level,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Size:      1,
		CreatedAt: manifestTestLastModified,
	})
	return manifest
}

func assertManifestEntries(t *testing.T, manifest *Manifest, level int, want [][2]ltx.TXID) {
	t.Helper()
	assertFileRanges(t, manifest.EntriesForLevel(level, 0), want)
}

func assertFileRanges(t *testing.T, infos []*ltx.FileInfo, want [][2]ltx.TXID) {
	t.Helper()
	if len(infos) != len(want) {
		t.Fatalf("file count = %d, want %d: %#v", len(infos), len(want), infos)
	}
	for i, info := range infos {
		if got := [2]ltx.TXID{info.MinTXID, info.MaxTXID}; got != want[i] {
			t.Fatalf("file %d range = %v, want %v", i, got, want[i])
		}
	}
}

func mustCollectLTXFiles(t *testing.T, client *ReplicaClient, level int) []*ltx.FileInfo {
	t.Helper()
	itr, err := client.LTXFiles(context.Background(), level, 0, false)
	if err != nil {
		t.Fatalf("list LTX files: %v", err)
	}
	defer func() {
		if err := itr.Close(); err != nil {
			t.Fatalf("close LTX iterator: %v", err)
		}
	}()

	var infos []*ltx.FileInfo
	for itr.Next() {
		infos = append(infos, itr.Item())
	}
	if err := itr.Err(); err != nil {
		t.Fatalf("iterate LTX files: %v", err)
	}
	return infos
}

func assertManifestListFallback(t *testing.T, store *manifestTestStore, path string, level int, want [][2]ltx.TXID) {
	t.Helper()
	listCount := store.operationCount("ListObjectsV2")
	reader := store.newClient(path)
	reader.ManifestEnabled = true
	assertFileRanges(t, mustCollectLTXFiles(t, reader, level), want)
	if got := store.operationCount("ListObjectsV2"); got <= listCount {
		t.Fatal("expected LIST fallback for invalid or missing manifest")
	}
}

func TestReplicaClient_ManifestBootstrapExistingReplica(t *testing.T) {
	store := newManifestTestStore(t)
	store.putLTX(t, "db", 0, 1, 1)
	store.putLTX(t, "db", 1, 1, 5)

	client := store.newClient("db")
	client.ManifestWriteEnabled = true
	if _, err := client.WriteLTXFile(context.Background(), 0, 6, 6, bytes.NewReader(mustManifestTestLTX(t, 6, 6))); err != nil {
		t.Fatal(err)
	}

	manifest := store.manifest(t, "db")
	assertManifestEntries(t, manifest, 0, [][2]ltx.TXID{{1, 1}, {6, 6}})
	assertManifestEntries(t, manifest, 1, [][2]ltx.TXID{{1, 5}})
}

func TestReplicaClient_ManifestInvalidationForcesListFallback(t *testing.T) {
	store := newManifestTestStore(t)
	store.putLTX(t, "db", 0, 1, 1)
	writer := store.newClient("db")
	writer.ManifestWriteEnabled = true

	writer.manifestMu.Lock()
	ready, err := writer.prepareManifestMutation(context.Background(), true)
	writer.manifestMu.Unlock()
	if err != nil || !ready {
		t.Fatalf("prepare manifest mutation: ready=%v err=%v", ready, err)
	}

	listCount := store.operationCount("ListObjectsV2")
	reader := store.newClient("db")
	reader.ManifestEnabled = true
	infos := mustCollectLTXFiles(t, reader, 0)
	assertFileRanges(t, infos, [][2]ltx.TXID{{1, 1}})
	if got := store.operationCount("ListObjectsV2"); got <= listCount {
		t.Fatal("expected LIST fallback for invalid manifest")
	}
}

func TestReplicaClient_ManifestMutationFailures(t *testing.T) {
	t.Run("UploadFailureLeavesInvalidManifest", testManifestUploadFailure)
	t.Run("ManifestPutFailureLeavesInvalidManifest", testManifestPublishFailure)
	t.Run("PartialDeleteLeavesInvalidManifest", testManifestPartialDeleteFailure)
	t.Run("RestartRebuildsAfterInvalidManifest", testManifestRestartRebuild)
	t.Run("RebuildListFailureLeavesInvalidManifest", testManifestRebuildFailure)
}

func testManifestUploadFailure(t *testing.T) {
	store := newManifestTestStore(t)
	store.putLTX(t, "db", 0, 1, 1)
	store.failNext("PutObject", nil)
	store.failNext("PutObject", errors.New("upload unavailable"))

	writer := store.newClient("db")
	writer.ManifestWriteEnabled = true
	if _, err := writer.WriteLTXFile(context.Background(), 0, 2, 2, bytes.NewReader(mustManifestTestLTX(t, 2, 2))); err == nil {
		t.Fatal("expected upload error")
	}
	if store.hasLTX("db", 0, 2, 2) {
		t.Fatal("LTX file exists after failed upload")
	}
	if got := store.manifest(t, "db").Version; got != manifestInvalidVersion {
		t.Fatalf("manifest version = %d, want %d", got, manifestInvalidVersion)
	}
	assertManifestListFallback(t, store, "db", 0, [][2]ltx.TXID{{1, 1}})
}

func testManifestPublishFailure(t *testing.T) {
	store := newManifestTestStore(t)
	store.putLTX(t, "db", 0, 1, 1)
	store.failNext("PutObject", nil)
	store.failNext("PutObject", nil)
	store.failNext("PutObject", errors.New("manifest unavailable"))

	writer := store.newClient("db")
	writer.ManifestWriteEnabled = true
	if _, err := writer.WriteLTXFile(context.Background(), 0, 2, 2, bytes.NewReader(mustManifestTestLTX(t, 2, 2))); err != nil {
		t.Fatal(err)
	}
	if got := store.manifest(t, "db").Version; got != manifestInvalidVersion {
		t.Fatalf("manifest version = %d, want %d", got, manifestInvalidVersion)
	}
	assertManifestListFallback(t, store, "db", 0, [][2]ltx.TXID{{1, 1}, {2, 2}})
}

func testManifestPartialDeleteFailure(t *testing.T) {
	store := newManifestTestStore(t)
	store.putLTX(t, "db", 0, 1, 1)
	store.putLTX(t, "db", 0, 2, 2)
	manifest := manifestWithFile(0, 1, 1)
	manifest.AddFile(manifestWithFile(0, 2, 2).EntriesForLevel(0, 0)[0])
	store.putManifest(t, "db", manifest)
	store.failNextDeleteObjects(manifestTestDeleteError{
		Key:     "db/0000/0000000000000002-0000000000000002.ltx",
		Code:    "InternalError",
		Message: "delete unavailable",
	})

	writer := store.newClient("db")
	writer.ManifestWriteEnabled = true
	err := writer.DeleteLTXFiles(context.Background(), []*ltx.FileInfo{
		{Level: 0, MinTXID: 1, MaxTXID: 1},
		{Level: 0, MinTXID: 2, MaxTXID: 2},
	})
	if err == nil {
		t.Fatal("expected partial delete error")
	}
	if store.hasLTX("db", 0, 1, 1) {
		t.Fatal("successfully deleted LTX file still exists")
	}
	if !store.hasLTX("db", 0, 2, 2) {
		t.Fatal("failed LTX deletion removed object")
	}
	if got := store.manifest(t, "db").Version; got != manifestInvalidVersion {
		t.Fatalf("manifest version = %d, want %d", got, manifestInvalidVersion)
	}
	assertManifestListFallback(t, store, "db", 0, [][2]ltx.TXID{{2, 2}})
}

func testManifestRestartRebuild(t *testing.T) {
	store := newManifestTestStore(t)
	store.failNext("PutObject", nil)
	store.failNext("PutObject", nil)
	store.failNext("PutObject", errors.New("manifest unavailable"))

	writer := store.newClient("db")
	writer.ManifestWriteEnabled = true
	if _, err := writer.WriteLTXFile(context.Background(), 0, 1, 1, bytes.NewReader(mustManifestTestLTX(t, 1, 1))); err != nil {
		t.Fatal(err)
	}
	assertManifestListFallback(t, store, "db", 0, [][2]ltx.TXID{{1, 1}})

	restartedWriter := store.newClient("db")
	restartedWriter.ManifestWriteEnabled = true
	if _, err := restartedWriter.WriteLTXFile(context.Background(), 0, 2, 2, bytes.NewReader(mustManifestTestLTX(t, 2, 2))); err != nil {
		t.Fatal(err)
	}
	manifest := store.manifest(t, "db")
	if got := manifest.Version; got != ManifestVersion {
		t.Fatalf("manifest version = %d, want %d", got, ManifestVersion)
	}
	assertManifestEntries(t, manifest, 0, [][2]ltx.TXID{{1, 1}, {2, 2}})
}

func testManifestRebuildFailure(t *testing.T) {
	store := newManifestTestStore(t)
	store.putLTX(t, "db", 0, 1, 1)
	store.failNext("ListObjectsV2", errors.New("list unavailable"))

	writer := store.newClient("db")
	writer.ManifestWriteEnabled = true
	if _, err := writer.WriteLTXFile(context.Background(), 0, 2, 2, bytes.NewReader(mustManifestTestLTX(t, 2, 2))); err != nil {
		t.Fatal(err)
	}
	if got := store.manifest(t, "db").Version; got != manifestInvalidVersion {
		t.Fatalf("manifest version = %d, want %d", got, manifestInvalidVersion)
	}
	assertManifestListFallback(t, store, "db", 0, [][2]ltx.TXID{{1, 1}, {2, 2}})
}

func TestReplicaClient_ManifestCleanupRetriesBeforeMutation(t *testing.T) {
	store := newManifestTestStore(t)
	store.putLTX(t, "db", 0, 1, 1)
	store.putManifest(t, "db", manifestWithFile(0, 1, 1))
	writer := store.newClient("db")
	writer.ManifestConfigured = true
	store.failNext("DeleteObject", errors.New("cleanup unavailable"))

	_, err := writer.WriteLTXFile(context.Background(), 0, 2, 2, bytes.NewReader(mustManifestTestLTX(t, 2, 2)))
	if err == nil {
		t.Fatal("expected cleanup error")
	}
	if store.hasLTX("db", 0, 2, 2) {
		t.Fatal("LTX mutation occurred before stale manifest cleanup")
	}
	if !store.hasObject("db/manifest.json") {
		t.Fatal("manifest removed after failed cleanup")
	}

	if _, err := writer.WriteLTXFile(context.Background(), 0, 2, 2, bytes.NewReader(mustManifestTestLTX(t, 2, 2))); err != nil {
		t.Fatal(err)
	}
	if !store.hasLTX("db", 0, 2, 2) {
		t.Fatal("expected retried mutation to succeed")
	}
	if store.hasObject("db/manifest.json") {
		t.Fatal("stale manifest remains after successful cleanup")
	}
	if got := store.operationCount("DeleteObject"); got != 2 {
		t.Fatalf("cleanup attempts=%d, want 2", got)
	}
}

func TestReplicaClient_ManifestDeleteLTXFilesPublishesSurvivors(t *testing.T) {
	store := newManifestTestStore(t)
	store.putLTX(t, "db", 0, 1, 1)
	store.putLTX(t, "db", 0, 2, 2)
	manifest := manifestWithFile(0, 1, 1)
	manifest.AddFile(manifestWithFile(0, 2, 2).EntriesForLevel(0, 0)[0])
	store.putManifest(t, "db", manifest)

	writer := store.newClient("db")
	writer.ManifestWriteEnabled = true
	if err := writer.DeleteLTXFiles(context.Background(), []*ltx.FileInfo{{Level: 0, MinTXID: 1, MaxTXID: 1}}); err != nil {
		t.Fatal(err)
	}
	manifest = store.manifest(t, "db")
	if got := manifest.Version; got != ManifestVersion {
		t.Fatalf("manifest version = %d, want %d", got, ManifestVersion)
	}
	assertManifestEntries(t, manifest, 0, [][2]ltx.TXID{{2, 2}})

	listCount := store.operationCount("ListObjectsV2")
	reader := store.newClient("db")
	reader.ManifestEnabled = true
	assertFileRanges(t, mustCollectLTXFiles(t, reader, 0), [][2]ltx.TXID{{2, 2}})
	if got := store.operationCount("ListObjectsV2"); got != listCount {
		t.Fatal("valid manifest reader unexpectedly used LIST")
	}
}

func TestReplicaClient_ManifestEnabledDeleteLTXFiles(t *testing.T) {
	t.Run("SuccessLeavesInvalidManifest", func(t *testing.T) {
		store := newManifestTestStore(t)
		store.putLTX(t, "db", 0, 1, 1)
		store.putLTX(t, "db", 0, 2, 2)
		manifest := manifestWithFile(0, 1, 1)
		manifest.AddFile(manifestWithFile(0, 2, 2).EntriesForLevel(0, 0)[0])
		store.putManifest(t, "db", manifest)

		client := store.newClient("db")
		client.ManifestEnabled = true
		client.manifest = manifest
		if err := client.DeleteLTXFiles(context.Background(), []*ltx.FileInfo{{Level: 0, MinTXID: 1, MaxTXID: 1}}); err != nil {
			t.Fatal(err)
		}
		if store.hasLTX("db", 0, 1, 1) {
			t.Fatal("deleted LTX file still exists")
		}
		if got := store.manifest(t, "db").Version; got != manifestInvalidVersion {
			t.Fatalf("manifest version = %d, want %d", got, manifestInvalidVersion)
		}
		if client.manifest != nil {
			t.Fatal("manifest cache retained after deletion")
		}
		assertManifestListFallback(t, store, "db", 0, [][2]ltx.TXID{{2, 2}})
	})

	t.Run("FailureLeavesInvalidManifest", func(t *testing.T) {
		store := newManifestTestStore(t)
		store.putLTX(t, "db", 0, 1, 1)
		manifest := manifestWithFile(0, 1, 1)
		store.putManifest(t, "db", manifest)
		store.failNext("DeleteObjects", errors.New("delete unavailable"))

		client := store.newClient("db")
		client.ManifestEnabled = true
		client.manifest = manifest
		if err := client.DeleteLTXFiles(context.Background(), []*ltx.FileInfo{{Level: 0, MinTXID: 1, MaxTXID: 1}}); err == nil {
			t.Fatal("expected delete error")
		}
		if !store.hasLTX("db", 0, 1, 1) {
			t.Fatal("failed deletion removed LTX file")
		}
		if got := store.manifest(t, "db").Version; got != manifestInvalidVersion {
			t.Fatalf("manifest version = %d, want %d", got, manifestInvalidVersion)
		}
		if client.manifest != nil {
			t.Fatal("manifest cache retained after failed deletion")
		}
		assertManifestListFallback(t, store, "db", 0, [][2]ltx.TXID{{1, 1}})
	})

	t.Run("InvalidationFailureBlocksMutation", func(t *testing.T) {
		store := newManifestTestStore(t)
		store.putLTX(t, "db", 0, 1, 1)
		store.putManifest(t, "db", manifestWithFile(0, 1, 1))
		store.failNext("PutObject", errors.New("invalidation unavailable"))

		client := store.newClient("db")
		client.ManifestEnabled = true
		if err := client.DeleteLTXFiles(context.Background(), []*ltx.FileInfo{{Level: 0, MinTXID: 1, MaxTXID: 1}}); err == nil {
			t.Fatal("expected invalidation error")
		}
		if !store.hasLTX("db", 0, 1, 1) {
			t.Fatal("LTX mutation occurred before manifest invalidation")
		}
		if got := store.operationCount("DeleteObjects"); got != 0 {
			t.Fatalf("delete operations=%d, want 0", got)
		}
	})
}

func TestReplicaClient_ManifestEnabledDeleteAll(t *testing.T) {
	t.Run("SuccessRemovesManifest", func(t *testing.T) {
		store := newManifestTestStore(t)
		store.putLTX(t, "db", 0, 1, 1)
		manifest := manifestWithFile(0, 1, 1)
		store.putManifest(t, "db", manifest)

		client := store.newClient("db")
		client.ManifestEnabled = true
		client.manifest = manifest
		if err := client.DeleteAll(context.Background()); err != nil {
			t.Fatal(err)
		}
		if store.hasLTX("db", 0, 1, 1) || store.hasObject("db/manifest.json") {
			t.Fatal("DeleteAll left replica objects")
		}
		if client.manifest != nil {
			t.Fatal("manifest cache retained after DeleteAll")
		}
		if got := store.operationCount("PutObject"); got != 1 {
			t.Fatalf("manifest invalidations=%d, want 1", got)
		}
		assertManifestListFallback(t, store, "db", 0, nil)
	})

	t.Run("FailureLeavesInvalidManifest", func(t *testing.T) {
		store := newManifestTestStore(t)
		store.putLTX(t, "db", 0, 1, 1)
		manifest := manifestWithFile(0, 1, 1)
		store.putManifest(t, "db", manifest)
		store.failNext("DeleteObjects", errors.New("delete unavailable"))

		client := store.newClient("db")
		client.ManifestEnabled = true
		client.manifest = manifest
		if err := client.DeleteAll(context.Background()); err == nil {
			t.Fatal("expected delete error")
		}
		if !store.hasLTX("db", 0, 1, 1) {
			t.Fatal("failed DeleteAll removed LTX file")
		}
		if got := store.manifest(t, "db").Version; got != manifestInvalidVersion {
			t.Fatalf("manifest version = %d, want %d", got, manifestInvalidVersion)
		}
		if client.manifest != nil {
			t.Fatal("manifest cache retained after failed DeleteAll")
		}
		assertManifestListFallback(t, store, "db", 0, [][2]ltx.TXID{{1, 1}})
	})

	t.Run("InvalidationFailureBlocksMutation", func(t *testing.T) {
		store := newManifestTestStore(t)
		store.putLTX(t, "db", 0, 1, 1)
		store.putManifest(t, "db", manifestWithFile(0, 1, 1))
		store.failNext("PutObject", errors.New("invalidation unavailable"))

		client := store.newClient("db")
		client.ManifestEnabled = true
		if err := client.DeleteAll(context.Background()); err == nil {
			t.Fatal("expected invalidation error")
		}
		if !store.hasLTX("db", 0, 1, 1) {
			t.Fatal("DeleteAll mutated objects before manifest invalidation")
		}
		if got := store.operationCount("ListObjectsV2"); got != 0 {
			t.Fatalf("list operations=%d, want 0", got)
		}
		if got := store.operationCount("DeleteObjects"); got != 0 {
			t.Fatalf("delete operations=%d, want 0", got)
		}
	})
}

func TestReplicaClient_ManifestDisabledCleanupDeletionRetries(t *testing.T) {
	t.Run("DeleteLTXFiles", func(t *testing.T) {
		store := newManifestTestStore(t)
		store.putLTX(t, "db", 0, 1, 1)
		store.putManifest(t, "db", manifestWithFile(0, 1, 1))
		store.failNext("DeleteObject", errors.New("cleanup unavailable"))

		writer := store.newClient("db")
		writer.ManifestConfigured = true
		files := []*ltx.FileInfo{{Level: 0, MinTXID: 1, MaxTXID: 1}}
		if err := writer.DeleteLTXFiles(context.Background(), files); err == nil {
			t.Fatal("expected cleanup error")
		}
		if !store.hasLTX("db", 0, 1, 1) {
			t.Fatal("LTX mutation occurred before stale manifest cleanup")
		}
		if err := writer.DeleteLTXFiles(context.Background(), files); err != nil {
			t.Fatal(err)
		}
		if store.hasLTX("db", 0, 1, 1) {
			t.Fatal("expected retried deletion to succeed")
		}
		if got := store.operationCount("DeleteObject"); got != 2 {
			t.Fatalf("cleanup attempts=%d, want 2", got)
		}
	})

	t.Run("DeleteAll", func(t *testing.T) {
		store := newManifestTestStore(t)
		store.putLTX(t, "db", 0, 1, 1)
		store.putManifest(t, "db", manifestWithFile(0, 1, 1))
		store.failNext("DeleteObject", errors.New("cleanup unavailable"))

		writer := store.newClient("db")
		writer.ManifestConfigured = true
		if err := writer.DeleteAll(context.Background()); err == nil {
			t.Fatal("expected cleanup error")
		}
		if !store.hasLTX("db", 0, 1, 1) {
			t.Fatal("DeleteAll mutated objects before stale manifest cleanup")
		}
		if err := writer.DeleteAll(context.Background()); err != nil {
			t.Fatal(err)
		}
		if store.hasLTX("db", 0, 1, 1) || store.hasObject("db/manifest.json") {
			t.Fatal("expected retried DeleteAll to succeed")
		}
		if got := store.operationCount("DeleteObject"); got != 2 {
			t.Fatalf("cleanup attempts=%d, want 2", got)
		}
	})
}

func TestReplicaClient_ManifestDeleteAllLifecycle(t *testing.T) {
	t.Run("SuccessRemovesSentinelAndObjects", func(t *testing.T) {
		store := newManifestTestStore(t)
		store.putLTX(t, "db", 0, 1, 1)
		store.putManifest(t, "db", manifestWithFile(0, 1, 1))

		writer := store.newClient("db")
		writer.ManifestWriteEnabled = true
		if err := writer.DeleteAll(context.Background()); err != nil {
			t.Fatal(err)
		}
		if store.hasLTX("db", 0, 1, 1) || store.hasObject("db/manifest.json") {
			t.Fatal("DeleteAll left replica objects")
		}
		if writer.manifest != nil {
			t.Fatal("DeleteAll retained in-memory manifest")
		}
		assertManifestListFallback(t, store, "db", 0, nil)
	})

	t.Run("FailureLeavesInvalidManifest", func(t *testing.T) {
		store := newManifestTestStore(t)
		store.putLTX(t, "db", 0, 1, 1)
		store.putManifest(t, "db", manifestWithFile(0, 1, 1))
		store.failNext("DeleteObjects", errors.New("delete unavailable"))

		writer := store.newClient("db")
		writer.ManifestWriteEnabled = true
		if err := writer.DeleteAll(context.Background()); err == nil {
			t.Fatal("expected delete error")
		}
		if !store.hasLTX("db", 0, 1, 1) {
			t.Fatal("failed DeleteAll removed LTX object")
		}
		if got := store.manifest(t, "db").Version; got != manifestInvalidVersion {
			t.Fatalf("manifest version = %d, want %d", got, manifestInvalidVersion)
		}
		if writer.manifest != nil {
			t.Fatal("failed DeleteAll retained in-memory manifest")
		}
		assertManifestListFallback(t, store, "db", 0, [][2]ltx.TXID{{1, 1}})
	})
}

func TestManifestConsistencyStore(t *testing.T) {
	store := newManifestTestStore(t)
	store.putLTX(t, "seed", 0, 1, 1)
	if !store.hasLTX("seed", 0, 1, 1) {
		t.Fatal("seed LTX object not found")
	}
	store.putManifest(t, "seed", NewManifest())
	if got := store.manifest(t, "seed"); got.Version != ManifestVersion {
		t.Fatalf("manifest version = %d, want %d", got.Version, ManifestVersion)
	}

	client := store.newClient("test-path")
	ctx := context.Background()
	data := mustLTX(t)

	info, err := client.WriteLTXFile(ctx, 0, 2, 2, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("write LTX file: %v", err)
	}

	key := "test-path/0000/0000000000000002-0000000000000002.ltx"
	if got := store.object(t, key); !bytes.Equal(got, data) {
		t.Fatalf("stored object mismatch")
	}

	r, err := client.OpenLTXFile(ctx, 0, 2, 2, 0, 0)
	if err != nil {
		t.Fatalf("open LTX file: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read LTX file: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close LTX file: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("read object mismatch")
	}

	itr, err := client.LTXFiles(ctx, 0, 0, false)
	if err != nil {
		t.Fatalf("list LTX files: %v", err)
	}
	if !itr.Next() {
		t.Fatalf("expected listed LTX file: %v", itr.Err())
	}
	if got, want := itr.Item(), info; got.Level != want.Level || got.MinTXID != want.MinTXID || got.MaxTXID != want.MaxTXID || got.Size != want.Size {
		t.Fatalf("listed LTX file = %#v, want %#v", got, want)
	}
	if itr.Next() {
		t.Fatalf("unexpected additional LTX file: %#v", itr.Item())
	}
	if err := itr.Err(); err != nil {
		t.Fatalf("iterate LTX files: %v", err)
	}
	if err := itr.Close(); err != nil {
		t.Fatalf("close LTX iterator: %v", err)
	}

	if err := client.DeleteLTXFiles(ctx, []*ltx.FileInfo{info}); err != nil {
		t.Fatalf("delete LTX file: %v", err)
	}
	if store.hasObject(key) {
		t.Fatalf("object still exists after delete")
	}

	for operation, want := range map[string]int{
		"PutObject":     1,
		"GetObject":     1,
		"ListObjectsV2": 1,
		"DeleteObjects": 1,
	} {
		if got := store.operationCount(operation); got != want {
			t.Fatalf("%s count = %d, want %d", operation, got, want)
		}
	}

	pagedStore := newManifestTestStore(t)
	pagedStore.putObject("paged/a", []byte("a"))
	pagedStore.putObject("paged/b", []byte("bb"))
	pagedStore.putObject("paged/c", []byte("ccc"))
	pagedClient := pagedStore.newClient("paged")
	paginator := awss3.NewListObjectsV2Paginator(pagedClient.s3, &awss3.ListObjectsV2Input{
		Bucket:  aws.String(manifestTestBucket),
		Prefix:  aws.String("paged/"),
		MaxKeys: aws.Int32(1),
	})
	var listedKeys []string
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			t.Fatalf("list paginated objects: %v", err)
		}
		if len(page.Contents) != 1 {
			t.Fatalf("paginated object count = %d, want 1", len(page.Contents))
		}
		object := page.Contents[0]
		listedKeys = append(listedKeys, aws.ToString(object.Key))
		if aws.ToString(object.ETag) == "" || aws.ToInt64(object.Size) == 0 || !aws.ToTime(object.LastModified).Equal(manifestTestLastModified) {
			t.Fatalf("incomplete paginated object: %#v", object)
		}
	}
	if got, want := strings.Join(listedKeys, ","), "paged/a,paged/b,paged/c"; got != want {
		t.Fatalf("paginated keys = %q, want %q", got, want)
	}
	if got := pagedStore.operationCount("ListObjectsV2"); got != 3 {
		t.Fatalf("paginated ListObjectsV2 count = %d, want 3", got)
	}
	if _, err := pagedClient.s3.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: aws.String(manifestTestBucket),
		Key:    aws.String("paged/a"),
	}); err != nil {
		t.Fatalf("delete single object: %v", err)
	}
	if pagedStore.hasObject("paged/a") {
		t.Fatal("single object still exists after delete")
	}
	if got := pagedStore.operationCount("DeleteObject"); got != 1 {
		t.Fatalf("DeleteObject count = %d, want 1", got)
	}

	failedStore := newManifestTestStore(t)
	failedStore.failNext("PutObject", errors.New("put unavailable"))
	if _, err := failedStore.newClient("failed").WriteLTXFile(ctx, 0, 2, 2, bytes.NewReader(data)); err == nil {
		t.Fatal("expected injected PutObject failure")
	}
	if got := failedStore.operationCount("PutObject"); got != 1 {
		t.Fatalf("failed PutObject count = %d, want 1", got)
	}
}

func TestManifestConsistencyStoreListMaxKeysZero(t *testing.T) {
	store := newManifestTestStore(t)
	store.putObject("paged/a", []byte("a"))
	store.putObject("paged/b", []byte("b"))

	request, err := http.NewRequest(http.MethodGet, "https://example.com?prefix=paged%2F&max-keys=0&continuation-token=0", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := store.listObjectsV2Response(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()

	var result manifestTestListResult
	if err := xml.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if got, want := result.NextContinuationToken, "1"; got != want {
		t.Fatalf("next continuation token = %q, want %q", got, want)
	}
}
