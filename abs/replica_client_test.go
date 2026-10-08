package abs

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
)

func TestReplicaClient_LTXFilesSeek(t *testing.T) {
	names := []string{
		litestream.LTXFilePath("integration", 0, 1, 1),
		litestream.LTXFilePath("integration", 0, 3, 3),
		litestream.LTXFilePath("integration", 0, 5, 5),
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedPrefix := r.URL.Query().Get("prefix")
		response := listBlobsResponse{
			ServiceEndpoint: serverURL(r),
			ContainerName:   "litestream-test",
		}
		for _, name := range names {
			if strings.HasPrefix(name, requestedPrefix) {
				response.Blobs = append(response.Blobs, newListBlob(name))
			}
		}

		w.Header().Set("Content-Type", "application/xml")
		if err := xml.NewEncoder(w).Encode(response); err != nil {
			t.Errorf("encode response: %v", err)
		}
	}))
	t.Cleanup(server.Close)

	client, err := azblob.NewClientWithNoCredential(server.URL, nil)
	if err != nil {
		t.Fatalf("NewClientWithNoCredential: %v", err)
	}
	rc := NewReplicaClient()
	rc.client = client
	rc.Bucket = "litestream-test"
	rc.Path = "integration"

	for _, tt := range []struct {
		name string
		seek ltx.TXID
		want []ltx.TXID
	}{
		{name: "exact", seek: 3, want: []ltx.TXID{3, 5}},
		{name: "next available", seek: 2, want: []ltx.TXID{3, 5}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			itr, err := rc.LTXFiles(t.Context(), 0, tt.seek, false)
			if err != nil {
				t.Fatalf("LTXFiles: %v", err)
			}

			var got []ltx.TXID
			for itr.Next() {
				got = append(got, itr.Item().MinTXID)
			}
			if err := itr.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("MinTXIDs=%v, want %v", got, tt.want)
			}
		})
	}
}

type listBlobsResponse struct {
	XMLName         xml.Name   `xml:"EnumerationResults"`
	ServiceEndpoint string     `xml:"ServiceEndpoint,attr"`
	ContainerName   string     `xml:"ContainerName,attr"`
	Blobs           []listBlob `xml:"Blobs>Blob"`
	NextMarker      string     `xml:"NextMarker"`
}

type listBlob struct {
	Name       string             `xml:"Name"`
	Properties listBlobProperties `xml:"Properties"`
}

type listBlobProperties struct {
	ETag          string `xml:"Etag"`
	LastModified  string `xml:"Last-Modified"`
	CreationTime  string `xml:"Creation-Time"`
	ContentLength int64  `xml:"Content-Length"`
}

func newListBlob(name string) listBlob {
	timestamp := time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC).Format(http.TimeFormat)
	return listBlob{
		Name: name,
		Properties: listBlobProperties{
			ETag:          `"etag"`,
			LastModified:  timestamp,
			CreationTime:  timestamp,
			ContentLength: 1,
		},
	}
}

func serverURL(r *http.Request) string {
	return "http://" + r.Host
}
