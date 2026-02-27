package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type COSIBucketInfo struct {
	Spec COSIBucketInfoSpec `json:"spec"`
}

type COSIBucketInfoSpec struct {
	BucketName         string          `json:"bucketName"`
	AuthenticationType string          `json:"authenticationType"`
	Protocols          json.RawMessage `json:"protocols"`
	SecretS3           *COSIS3Secret   `json:"secretS3"`
}

type COSIProtocols struct {
	S3 *COSIS3Protocol `json:"s3"`
}

type COSIS3Protocol struct {
	Endpoint         string `json:"endpoint"`
	Region           string `json:"region"`
	SignatureVersion string `json:"signatureVersion"`
}

type COSIS3Secret struct {
	Endpoint        string `json:"endpoint"`
	Region          string `json:"region"`
	AccessKeyID     string `json:"accessKeyID"`
	AccessSecretKey string `json:"accessSecretKey"`
}

func (spec *COSIBucketInfoSpec) ParseProtocols() (*COSIProtocols, error) {
	if len(spec.Protocols) == 0 {
		return &COSIProtocols{}, nil
	}

	// Try object form: {"s3": {...}}
	var obj COSIProtocols
	if err := json.Unmarshal(spec.Protocols, &obj); err == nil && obj.S3 != nil {
		return &obj, nil
	}

	// Try array form: ["S3"]
	var arr []string
	if err := json.Unmarshal(spec.Protocols, &arr); err == nil {
		for _, p := range arr {
			if strings.EqualFold(p, "s3") {
				return &COSIProtocols{}, nil
			}
		}
	}

	return &COSIProtocols{}, nil
}

func (spec *COSIBucketInfoSpec) HasS3() bool {
	if spec.SecretS3 != nil {
		return true
	}
	protocols, err := spec.ParseProtocols()
	if err != nil {
		return false
	}
	return protocols.S3 != nil
}

func ReadBucketInfo(path string) (*COSIBucketInfo, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read bucket info file: %w", err)
	}

	var info COSIBucketInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("parse bucket info JSON: %w", err)
	}

	if !info.Spec.HasS3() {
		return nil, fmt.Errorf("bucket info contains no S3 protocol configuration")
	}

	return &info, nil
}

func (rs *ReplicaSettings) ApplyBucketInfo(info *COSIBucketInfo, hasURL bool) {
	if info == nil {
		return
	}

	if s := info.Spec.SecretS3; s != nil {
		if rs.AccessKeyID == "" {
			rs.AccessKeyID = s.AccessKeyID
		}
		if rs.SecretAccessKey == "" {
			rs.SecretAccessKey = s.AccessSecretKey
		}
		if rs.Endpoint == "" && s.Endpoint != "" {
			rs.Endpoint = s.Endpoint
		}
		if rs.Region == "" && s.Region != "" {
			rs.Region = s.Region
		}
	}

	protocols, _ := info.Spec.ParseProtocols()
	if protocols != nil && protocols.S3 != nil {
		if rs.Endpoint == "" && protocols.S3.Endpoint != "" {
			rs.Endpoint = protocols.S3.Endpoint
		}
		if rs.Region == "" && protocols.S3.Region != "" {
			rs.Region = protocols.S3.Region
		}
	}

	if !hasURL && rs.Bucket == "" && info.Spec.BucketName != "" {
		rs.Bucket = info.Spec.BucketName
	}
}
