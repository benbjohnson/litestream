package oss

import (
	"errors"
	"testing"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
)

func TestReplicaClient_Type(t *testing.T) {
	c := NewReplicaClient()
	if got := c.Type(); got != ReplicaClientType {
		t.Errorf("Type() = %q, want %q", got, ReplicaClientType)
	}
	if got := c.Type(); got != "oss" {
		t.Errorf("Type() = %q, want %q", got, "oss")
	}
}

func TestReplicaClient_Init_BucketValidation(t *testing.T) {
	t.Run("EmptyBucket", func(t *testing.T) {
		c := NewReplicaClient()
		c.Bucket = "" // Empty bucket name
		c.Region = "cn-hangzhou"

		err := c.Init(t.Context())
		if err == nil {
			t.Fatal("expected error for empty bucket name")
		}
		if got := err.Error(); got != "oss: bucket name is required" {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("ValidBucketWithRegion", func(t *testing.T) {
		c := NewReplicaClient()
		c.Bucket = "test-bucket"
		c.Region = "cn-hangzhou"
		c.AccessKeyID = "test-key"
		c.AccessKeySecret = "test-secret"

		// Init should succeed (client will be created even without real credentials)
		err := c.Init(t.Context())
		if err != nil {
			t.Errorf("Init() should succeed with valid bucket: %v", err)
		}
	})

	t.Run("ValidBucketDefaultRegion", func(t *testing.T) {
		c := NewReplicaClient()
		c.Bucket = "test-bucket"
		// Region is empty, should use DefaultRegion
		c.AccessKeyID = "test-key"
		c.AccessKeySecret = "test-secret"

		err := c.Init(t.Context())
		if err != nil {
			t.Errorf("Init() should succeed with default region: %v", err)
		}
	})
}

func TestReplicaClient_Init_Idempotent(t *testing.T) {
	c := NewReplicaClient()
	c.Bucket = "test-bucket"
	c.AccessKeyID = "test-key"
	c.AccessKeySecret = "test-secret"

	// First init
	if err := c.Init(t.Context()); err != nil {
		t.Fatalf("first Init() failed: %v", err)
	}

	// Second init should be a no-op
	if err := c.Init(t.Context()); err != nil {
		t.Fatalf("second Init() failed: %v", err)
	}
}

func TestParseURL(t *testing.T) {
	tests := []struct {
		name       string
		url        string
		wantBucket string
		wantRegion string
		wantKey    string
		wantErr    bool
	}{
		{
			name:       "SimpleOSSURL",
			url:        "oss://my-bucket/path/to/file",
			wantBucket: "my-bucket",
			wantRegion: "",
			wantKey:    "path/to/file",
			wantErr:    false,
		},
		{
			name:       "OSSURLWithRegion",
			url:        "oss://my-bucket.oss-cn-hangzhou.aliyuncs.com/backup",
			wantBucket: "my-bucket",
			wantRegion: "cn-hangzhou",
			wantKey:    "backup",
			wantErr:    false,
		},
		{
			name:       "OSSURLNoPath",
			url:        "oss://my-bucket",
			wantBucket: "my-bucket",
			wantRegion: "",
			wantKey:    "",
			wantErr:    false,
		},
		{
			name:       "InvalidScheme",
			url:        "s3://my-bucket/path",
			wantBucket: "",
			wantRegion: "",
			wantKey:    "",
			wantErr:    true,
		},
		{
			name:       "HTTPScheme",
			url:        "http://my-bucket/path",
			wantBucket: "",
			wantRegion: "",
			wantKey:    "",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, region, key, err := ParseURL(tt.url)

			if (err != nil) != tt.wantErr {
				t.Errorf("ParseURL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err != nil {
				return
			}

			if bucket != tt.wantBucket {
				t.Errorf("bucket = %q, want %q", bucket, tt.wantBucket)
			}
			if region != tt.wantRegion {
				t.Errorf("region = %q, want %q", region, tt.wantRegion)
			}
			if key != tt.wantKey {
				t.Errorf("key = %q, want %q", key, tt.wantKey)
			}
		})
	}
}

func TestParseHost(t *testing.T) {
	tests := []struct {
		name       string
		host       string
		wantBucket string
		wantRegion string
	}{
		{
			name:       "StandardOSSURL",
			host:       "my-bucket.oss-cn-hangzhou.aliyuncs.com",
			wantBucket: "my-bucket",
			wantRegion: "cn-hangzhou",
		},
		{
			name:       "OSSURLBeijingRegion",
			host:       "test-bucket.oss-cn-beijing.aliyuncs.com",
			wantBucket: "test-bucket",
			wantRegion: "cn-beijing",
		},
		{
			name:       "OSSURLShanghaiRegion",
			host:       "data-bucket.oss-cn-shanghai.aliyuncs.com",
			wantBucket: "data-bucket",
			wantRegion: "cn-shanghai",
		},
		{
			name:       "InternalOSSURL",
			host:       "my-bucket.oss-cn-hangzhou-internal.aliyuncs.com",
			wantBucket: "my-bucket",
			wantRegion: "cn-hangzhou",
		},
		{
			name:       "InternalOSSURLBeijing",
			host:       "test-bucket.oss-cn-beijing-internal.aliyuncs.com",
			wantBucket: "test-bucket",
			wantRegion: "cn-beijing",
		},
		{
			name:       "SimpleBucketName",
			host:       "my-bucket",
			wantBucket: "my-bucket",
			wantRegion: "",
		},
		{
			name:       "BucketWithHyphens",
			host:       "my-test-bucket-2024.oss-cn-shenzhen.aliyuncs.com",
			wantBucket: "my-test-bucket-2024",
			wantRegion: "cn-shenzhen",
		},
		{
			name:       "OSSURLWithNumbers",
			host:       "bucket123.oss-cn-hangzhou.aliyuncs.com",
			wantBucket: "bucket123",
			wantRegion: "cn-hangzhou",
		},
		{
			name:       "OSSURLHongKong",
			host:       "hk-bucket.oss-cn-hongkong.aliyuncs.com",
			wantBucket: "hk-bucket",
			wantRegion: "cn-hongkong",
		},
		{
			name:       "OSSURLSingapore",
			host:       "sg-bucket.oss-ap-southeast-1.aliyuncs.com",
			wantBucket: "sg-bucket",
			wantRegion: "ap-southeast-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, region, _ := ParseHost(tt.host)

			if bucket != tt.wantBucket {
				t.Errorf("bucket = %q, want %q", bucket, tt.wantBucket)
			}
			if region != tt.wantRegion {
				t.Errorf("region = %q, want %q", region, tt.wantRegion)
			}
		})
	}
}

func TestIsNotExists(t *testing.T) {
	t.Run("NilError", func(t *testing.T) {
		if isNotExists(nil) {
			t.Error("isNotExists should return false for nil error")
		}
	})

	t.Run("RegularError", func(t *testing.T) {
		regularErr := errors.New("regular error")
		if isNotExists(regularErr) {
			t.Error("isNotExists should return false for regular error")
		}
	})

	t.Run("WrappedError", func(t *testing.T) {
		wrappedErr := errors.New("wrapped: something went wrong")
		if isNotExists(wrappedErr) {
			t.Error("isNotExists should return false for wrapped non-ServiceError")
		}
	})
}

func TestLtxPath(t *testing.T) {
	c := NewReplicaClient()
	c.Path = "backups"

	tests := []struct {
		level    int
		filename string
		want     string
	}{
		{0, "00000001-00000001.ltx", "backups/0000/00000001-00000001.ltx"},
		{1, "00000001-00000010.ltx", "backups/0001/00000001-00000010.ltx"},
		{15, "00000001-000000ff.ltx", "backups/000f/00000001-000000ff.ltx"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := c.ltxPath(tt.level, tt.filename)
			if got != tt.want {
				t.Errorf("ltxPath(%d, %q) = %q, want %q", tt.level, tt.filename, got, tt.want)
			}
		})
	}
}

func TestDeleteResultError(t *testing.T) {
	ptr := func(s string) *string { return &s }

	t.Run("NilResult", func(t *testing.T) {
		requested := []oss.DeleteObject{{Key: ptr("key1")}}
		if err := deleteResultError(requested, nil); err != nil {
			t.Errorf("expected nil error for nil result, got %v", err)
		}
	})

	t.Run("AllDeleted", func(t *testing.T) {
		requested := []oss.DeleteObject{
			{Key: ptr("key1")},
			{Key: ptr("key2")},
		}
		result := &oss.DeleteMultipleObjectsResult{
			DeletedObjects: []oss.DeletedInfo{
				{Key: ptr("key1")},
				{Key: ptr("key2")},
			},
		}
		if err := deleteResultError(requested, result); err != nil {
			t.Errorf("expected nil error when all deleted, got %v", err)
		}
	})

	t.Run("SomeNotDeleted", func(t *testing.T) {
		requested := []oss.DeleteObject{
			{Key: ptr("key1")},
			{Key: ptr("key2")},
			{Key: ptr("key3")},
		}
		result := &oss.DeleteMultipleObjectsResult{
			DeletedObjects: []oss.DeletedInfo{
				{Key: ptr("key1")},
				// key2 and key3 not deleted
			},
		}
		err := deleteResultError(requested, result)
		if err == nil {
			t.Fatal("expected error when some keys not deleted")
		}
		errStr := err.Error()
		if !contains(errStr, "key2") {
			t.Errorf("error should mention key2: %s", errStr)
		}
		if !contains(errStr, "key3") {
			t.Errorf("error should mention key3: %s", errStr)
		}
	})

	t.Run("EmptyRequested", func(t *testing.T) {
		requested := []oss.DeleteObject{}
		result := &oss.DeleteMultipleObjectsResult{}
		if err := deleteResultError(requested, result); err != nil {
			t.Errorf("expected nil error for empty requested, got %v", err)
		}
	})

	t.Run("NilKeyInRequested", func(t *testing.T) {
		requested := []oss.DeleteObject{
			{Key: nil}, // nil key should be skipped
			{Key: ptr("key1")},
		}
		result := &oss.DeleteMultipleObjectsResult{
			DeletedObjects: []oss.DeletedInfo{
				{Key: ptr("key1")},
			},
		}
		if err := deleteResultError(requested, result); err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
	})
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
