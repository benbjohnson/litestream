package main_test

import (
	"os"
	"path/filepath"
	"testing"

	main "github.com/benbjohnson/litestream/cmd/litestream"
)

const testBucketInfoJSON = `{
  "spec": {
    "bucketName": "my-cosi-bucket",
    "authenticationType": "KEY",
    "protocols": {
      "s3": {
        "endpoint": "https://s3.example.com",
        "region": "us-west-2",
        "signatureVersion": "s3v4"
      }
    },
    "secretS3": {
      "endpoint": "https://s3.example.com",
      "region": "us-west-2",
      "accessKeyID": "AKIAIOSFODNN7EXAMPLE",
      "accessSecretKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    }
  }
}`

const testBucketInfoArrayProtocolJSON = `{
  "spec": {
    "bucketName": "objectstorage",
    "authenticationType": "Key",
    "secretS3": {
      "endpoint": "us-mia-1.linodeobjects.com",
      "region": "us-mia",
      "accessKeyID": "LINODE-KEY",
      "accessSecretKey": "LINODE-SECRET"
    },
    "secretAzure": null,
    "protocols": [
      "S3"
    ]
  }
}`

func writeBucketInfo(t *testing.T, dir, content string) string {
	t.Helper()
	path := filepath.Join(dir, "BucketInfo")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestReadBucketInfo(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		path := writeBucketInfo(t, t.TempDir(), testBucketInfoJSON)

		info, err := main.ReadBucketInfo(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Spec.BucketName != "my-cosi-bucket" {
			t.Fatalf("unexpected bucket name: %s", info.Spec.BucketName)
		}
		if info.Spec.SecretS3.AccessKeyID != "AKIAIOSFODNN7EXAMPLE" {
			t.Fatalf("unexpected access key: %s", info.Spec.SecretS3.AccessKeyID)
		}
		protocols, err := info.Spec.ParseProtocols()
		if err != nil {
			t.Fatal(err)
		}
		if protocols.S3 == nil {
			t.Fatal("expected S3 protocol to be parsed")
		}
		if protocols.S3.Region != "us-west-2" {
			t.Fatalf("unexpected region: %s", protocols.S3.Region)
		}
	})

	t.Run("ArrayProtocols", func(t *testing.T) {
		path := writeBucketInfo(t, t.TempDir(), testBucketInfoArrayProtocolJSON)

		info, err := main.ReadBucketInfo(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Spec.BucketName != "objectstorage" {
			t.Fatalf("unexpected bucket name: %s", info.Spec.BucketName)
		}
		if info.Spec.SecretS3.AccessKeyID != "LINODE-KEY" {
			t.Fatalf("unexpected access key: %s", info.Spec.SecretS3.AccessKeyID)
		}
	})

	t.Run("FileNotFound", func(t *testing.T) {
		_, err := main.ReadBucketInfo("/nonexistent/BucketInfo")
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("InvalidJSON", func(t *testing.T) {
		path := writeBucketInfo(t, t.TempDir(), `{invalid`)
		_, err := main.ReadBucketInfo(path)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("NoS3Protocol", func(t *testing.T) {
		path := writeBucketInfo(t, t.TempDir(), `{"spec":{"bucketName":"test","protocols":{}}}`)
		_, err := main.ReadBucketInfo(path)
		if err == nil {
			t.Fatal("expected error for missing S3 protocol")
		}
	})

	t.Run("SecretS3Only", func(t *testing.T) {
		json := `{"spec":{"bucketName":"test","protocols":{},"secretS3":{"accessKeyID":"KEY","accessSecretKey":"SECRET"}}}`
		path := writeBucketInfo(t, t.TempDir(), json)
		info, err := main.ReadBucketInfo(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Spec.SecretS3.AccessKeyID != "KEY" {
			t.Fatalf("unexpected access key: %s", info.Spec.SecretS3.AccessKeyID)
		}
	})
}

func TestApplyBucketInfo(t *testing.T) {
	t.Run("ApplyToEmpty", func(t *testing.T) {
		path := writeBucketInfo(t, t.TempDir(), testBucketInfoJSON)
		info, err := main.ReadBucketInfo(path)
		if err != nil {
			t.Fatal(err)
		}

		var rs main.ReplicaSettings
		rs.ApplyBucketInfo(info, false)

		if rs.AccessKeyID != "AKIAIOSFODNN7EXAMPLE" {
			t.Fatalf("expected access key to be applied, got: %s", rs.AccessKeyID)
		}
		if rs.SecretAccessKey != "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" {
			t.Fatalf("expected secret key to be applied, got: %s", rs.SecretAccessKey)
		}
		if rs.Endpoint != "https://s3.example.com" {
			t.Fatalf("expected endpoint to be applied, got: %s", rs.Endpoint)
		}
		if rs.Region != "us-west-2" {
			t.Fatalf("expected region to be applied, got: %s", rs.Region)
		}
		if rs.Bucket != "my-cosi-bucket" {
			t.Fatalf("expected bucket to be applied, got: %s", rs.Bucket)
		}
	})

	t.Run("ExplicitSettingsPreserved", func(t *testing.T) {
		path := writeBucketInfo(t, t.TempDir(), testBucketInfoJSON)
		info, err := main.ReadBucketInfo(path)
		if err != nil {
			t.Fatal(err)
		}

		rs := main.ReplicaSettings{
			AccessKeyID:     "MY-KEY",
			SecretAccessKey: "MY-SECRET",
			Region:          "eu-west-1",
			Bucket:          "my-bucket",
			Endpoint:        "https://custom.endpoint.com",
		}
		rs.ApplyBucketInfo(info, false)

		if rs.AccessKeyID != "MY-KEY" {
			t.Fatalf("access key should be preserved, got: %s", rs.AccessKeyID)
		}
		if rs.SecretAccessKey != "MY-SECRET" {
			t.Fatalf("secret key should be preserved, got: %s", rs.SecretAccessKey)
		}
		if rs.Region != "eu-west-1" {
			t.Fatalf("region should be preserved, got: %s", rs.Region)
		}
		if rs.Bucket != "my-bucket" {
			t.Fatalf("bucket should be preserved, got: %s", rs.Bucket)
		}
		if rs.Endpoint != "https://custom.endpoint.com" {
			t.Fatalf("endpoint should be preserved, got: %s", rs.Endpoint)
		}
	})

	t.Run("NilInfo", func(t *testing.T) {
		var rs main.ReplicaSettings
		rs.ApplyBucketInfo(nil, false)
		if rs.AccessKeyID != "" {
			t.Fatal("expected no changes for nil info")
		}
	})

	t.Run("SkipBucketWhenURLPresent", func(t *testing.T) {
		path := writeBucketInfo(t, t.TempDir(), testBucketInfoJSON)
		info, err := main.ReadBucketInfo(path)
		if err != nil {
			t.Fatal(err)
		}

		var rs main.ReplicaSettings
		rs.ApplyBucketInfo(info, true)

		if rs.Bucket != "" {
			t.Fatalf("bucket should not be set when URL is present, got: %s", rs.Bucket)
		}
		if rs.AccessKeyID != "AKIAIOSFODNN7EXAMPLE" {
			t.Fatalf("access key should still be applied, got: %s", rs.AccessKeyID)
		}
	})

	t.Run("ArrayProtocols", func(t *testing.T) {
		path := writeBucketInfo(t, t.TempDir(), testBucketInfoArrayProtocolJSON)
		info, err := main.ReadBucketInfo(path)
		if err != nil {
			t.Fatal(err)
		}

		var rs main.ReplicaSettings
		rs.ApplyBucketInfo(info, false)

		if rs.AccessKeyID != "LINODE-KEY" {
			t.Fatalf("expected access key from secretS3, got: %s", rs.AccessKeyID)
		}
		if rs.Endpoint != "us-mia-1.linodeobjects.com" {
			t.Fatalf("expected endpoint from secretS3, got: %s", rs.Endpoint)
		}
		if rs.Region != "us-mia" {
			t.Fatalf("expected region from secretS3, got: %s", rs.Region)
		}
		if rs.Bucket != "objectstorage" {
			t.Fatalf("expected bucket from bucketName, got: %s", rs.Bucket)
		}
	})
}

func TestParseConfig_BucketInfo(t *testing.T) {
	dir := t.TempDir()
	bucketInfoPath := writeBucketInfo(t, dir, testBucketInfoJSON)

	configYAML := `
dbs:
  - path: /tmp/test.db
    replicas:
      - bucket-info: ` + bucketInfoPath + `
        path: my-prefix
`
	configPath := filepath.Join(dir, "litestream.yml")
	if err := os.WriteFile(configPath, []byte(configYAML), 0644); err != nil {
		t.Fatal(err)
	}

	config, err := main.ReadConfigFile(configPath, true)
	if err != nil {
		t.Fatal(err)
	}

	if len(config.DBs) != 1 {
		t.Fatalf("expected 1 db, got %d", len(config.DBs))
	}
	rc := config.DBs[0].Replicas[0]
	if rc.AccessKeyID != "AKIAIOSFODNN7EXAMPLE" {
		t.Fatalf("expected bucket info access key, got: %s", rc.AccessKeyID)
	}
	if rc.Bucket != "my-cosi-bucket" {
		t.Fatalf("expected bucket info bucket name, got: %s", rc.Bucket)
	}
	if rc.Region != "us-west-2" {
		t.Fatalf("expected bucket info region, got: %s", rc.Region)
	}
	if rc.Endpoint != "https://s3.example.com" {
		t.Fatalf("expected bucket info endpoint, got: %s", rc.Endpoint)
	}
}

func TestParseConfig_BucketInfoOverride(t *testing.T) {
	dir := t.TempDir()
	bucketInfoPath := writeBucketInfo(t, dir, testBucketInfoJSON)

	configYAML := `
dbs:
  - path: /tmp/test.db
    replicas:
      - bucket-info: ` + bucketInfoPath + `
        path: my-prefix
        access-key-id: OVERRIDE-KEY
        region: eu-central-1
`
	configPath := filepath.Join(dir, "litestream.yml")
	if err := os.WriteFile(configPath, []byte(configYAML), 0644); err != nil {
		t.Fatal(err)
	}

	config, err := main.ReadConfigFile(configPath, true)
	if err != nil {
		t.Fatal(err)
	}

	rc := config.DBs[0].Replicas[0]
	if rc.AccessKeyID != "OVERRIDE-KEY" {
		t.Fatalf("expected override access key, got: %s", rc.AccessKeyID)
	}
	if rc.Region != "eu-central-1" {
		t.Fatalf("expected override region, got: %s", rc.Region)
	}
	if rc.SecretAccessKey != "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" {
		t.Fatalf("expected bucket info secret key (not overridden), got: %s", rc.SecretAccessKey)
	}
}

func TestParseConfig_BucketInfoAutoType(t *testing.T) {
	dir := t.TempDir()
	bucketInfoPath := writeBucketInfo(t, dir, testBucketInfoJSON)

	configYAML := `
dbs:
  - path: /tmp/test.db
    replicas:
      - bucket-info: ` + bucketInfoPath + `
        path: my-prefix
`
	configPath := filepath.Join(dir, "litestream.yml")
	if err := os.WriteFile(configPath, []byte(configYAML), 0644); err != nil {
		t.Fatal(err)
	}

	config, err := main.ReadConfigFile(configPath, true)
	if err != nil {
		t.Fatal(err)
	}

	rc := config.DBs[0].Replicas[0]
	if rc.Type != "s3" {
		t.Fatalf("expected auto-detected type 's3', got: %s", rc.Type)
	}
}

func TestParseConfig_BucketInfoArrayProtocols(t *testing.T) {
	dir := t.TempDir()
	bucketInfoPath := writeBucketInfo(t, dir, testBucketInfoArrayProtocolJSON)

	configYAML := `
dbs:
  - path: /tmp/test.db
    replicas:
      - bucket-info: ` + bucketInfoPath + `
        path: my-prefix
`
	configPath := filepath.Join(dir, "litestream.yml")
	if err := os.WriteFile(configPath, []byte(configYAML), 0644); err != nil {
		t.Fatal(err)
	}

	config, err := main.ReadConfigFile(configPath, true)
	if err != nil {
		t.Fatal(err)
	}

	rc := config.DBs[0].Replicas[0]
	if rc.AccessKeyID != "LINODE-KEY" {
		t.Fatalf("expected access key, got: %s", rc.AccessKeyID)
	}
	if rc.Bucket != "objectstorage" {
		t.Fatalf("expected bucket, got: %s", rc.Bucket)
	}
	if rc.Type != "s3" {
		t.Fatalf("expected type 's3', got: %s", rc.Type)
	}
}
