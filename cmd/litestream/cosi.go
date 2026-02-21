package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type COSIBucketInfo struct {
	Spec COSIBucketInfoSpec `json:"spec"`
}

type COSIBucketInfoSpec struct {
	BucketName         string        `json:"bucketName"`
	AuthenticationType string        `json:"authenticationType"`
	Protocols          COSIProtocols `json:"protocols"`
	SecretS3           *COSIS3Secret `json:"secretS3"`
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

func ReadBucketInfo(path string) (*COSIBucketInfo, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read bucket info file: %w", err)
	}

	var info COSIBucketInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("parse bucket info JSON: %w", err)
	}

	if info.Spec.Protocols.S3 == nil && info.Spec.SecretS3 == nil {
		return nil, fmt.Errorf("bucket info contains no S3 protocol configuration")
	}

	return &info, nil
}

func (rs *ReplicaSettings) ApplyBucketInfo(info *COSIBucketInfo) {
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

	if p := info.Spec.Protocols.S3; p != nil {
		if rs.Endpoint == "" && p.Endpoint != "" {
			rs.Endpoint = p.Endpoint
		}
		if rs.Region == "" && p.Region != "" {
			rs.Region = p.Region
		}
	}

	if rs.Bucket == "" && info.Spec.BucketName != "" {
		rs.Bucket = info.Spec.BucketName
	}
}
