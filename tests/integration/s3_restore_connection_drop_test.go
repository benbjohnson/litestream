//go:build integration && docker

package integration

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// TestRestore_S3ConnectionDrop verifies restore can recover from dropped S3-compatible connections.
func TestRestore_S3ConnectionDrop(t *testing.T) {
	RequireBinaries(t)
	RequireDocker(t)

	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	networkName := startDockerNetwork(t)
	defer removeDockerNetwork(networkName)

	minioName := startMinioContainerForProxy(t, networkName)
	defer stopDockerContainer(minioName)

	toxiproxyName, toxiproxyAPIPort, toxiproxyProxyPort := startToxiproxyContainer(t, networkName)
	defer stopDockerContainer(toxiproxyName)

	bucket := fmt.Sprintf("litestream-test-%d", time.Now().UnixNano())
	createMinioBucket(t, networkName, minioName, bucket)

	proxyEndpoint := fmt.Sprintf("http://localhost:%s", toxiproxyProxyPort)
	proxyClient := newToxiproxyClient(t, fmt.Sprintf("http://localhost:%s", toxiproxyAPIPort))
	proxyClient.createProxy(t, "minio", "0.0.0.0:8666", fmt.Sprintf("%s:9000", minioName))

	replicaPath := fmt.Sprintf("restore-drop-%d", time.Now().UnixNano())
	replicaURL := fmt.Sprintf("s3://%s/%s", bucket, replicaPath)

	db := SetupTestDB(t, "s3-restore-connection-drop")
	defer db.Cleanup()

	if err := db.Create(); err != nil {
		t.Fatalf("create db: %v", err)
	}

	if err := db.Populate("100MB"); err != nil {
		t.Fatalf("populate db: %v", err)
	}

	configPath := writeS3Config(t, db.Path, replicaURL, proxyEndpoint)
	db.ReplicaURL = replicaURL
	if err := db.StartLitestreamWithConfig(configPath); err != nil {
		t.Fatalf("start litestream: %v", err)
	}

	time.Sleep(5 * time.Second)

	if err := insertLargeRows(db.Path, 5, 256*1024); err != nil {
		t.Fatalf("insert post-snapshot rows: %v", err)
	}

	time.Sleep(5 * time.Second)

	if err := db.StopLitestream(); err != nil {
		t.Fatalf("stop litestream: %v", err)
	}

	restorePath := filepath.Join(db.TempDir, "restored.db")
	restoreErr := make(chan error, 1)
	go func() {
		restoreErr <- db.Restore(restorePath)
	}()

	time.Sleep(200 * time.Millisecond)
	proxyClient.addResetPeerToxic(t, "minio", "reset-connection", 200)
	time.Sleep(400 * time.Millisecond)
	proxyClient.removeToxic(t, "minio", "reset-connection")

	if err := <-restoreErr; err != nil {
		t.Fatalf("restore failed: %v", err)
	}

	if err := verifyRestoredRowCount(restorePath, 5); err != nil {
		t.Fatalf("restore validation failed: %v", err)
	}
}

func startDockerNetwork(t *testing.T) string {
	t.Helper()
	name := fmt.Sprintf("litestream-net-%d", time.Now().UnixNano())
	runDockerCommand(t, "network", "create", name)
	return name
}

func removeDockerNetwork(name string) {
	if name == "" {
		return
	}
	exec.Command("docker", "network", "rm", name).Run()
}

func startMinioContainerForProxy(t *testing.T, networkName string) string {
	t.Helper()
	name := fmt.Sprintf("litestream-minio-%d", time.Now().UnixNano())
	exec.Command("docker", "rm", "-f", name).Run()

	runDockerCommand(t, "run", "-d",
		"--name", name,
		"--network", networkName,
		"-e", "MINIO_ROOT_USER=minioadmin",
		"-e", "MINIO_ROOT_PASSWORD=minioadmin",
		"minio/minio", "server", "/data",
	)

	time.Sleep(3 * time.Second)
	return name
}

func startToxiproxyContainer(t *testing.T, networkName string) (string, string, string) {
	t.Helper()
	name := fmt.Sprintf("litestream-toxiproxy-%d", time.Now().UnixNano())
	exec.Command("docker", "rm", "-f", name).Run()

	image := os.Getenv("LITESTREAM_TOXIPROXY_IMAGE")
	if image == "" {
		image = "ghcr.io/shopify/toxiproxy:2.5.0"
	}

	runDockerCommand(t, "run", "-d",
		"--name", name,
		"--network", networkName,
		"-p", "0:8474",
		"-p", "0:8666",
		image,
	)

	apiPort := parseDockerPort(t, runDockerCommand(t, "port", name, "8474/tcp"))
	proxyPort := parseDockerPort(t, runDockerCommand(t, "port", name, "8666/tcp"))

	time.Sleep(2 * time.Second)

	return name, apiPort, proxyPort
}

func stopDockerContainer(name string) {
	if name == "" {
		return
	}
	exec.Command("docker", "rm", "-f", name).Run()
}

func createMinioBucket(t *testing.T, networkName, minioName, bucket string) {
	t.Helper()
	cmd := exec.Command("docker", "run", "--rm",
		"--network", networkName,
		"-e", fmt.Sprintf("MC_HOST_minio=http://minioadmin:minioadmin@%s:9000", minioName),
		"minio/mc", "mb", "minio/"+bucket,
	)
	output, err := cmd.CombinedOutput()
	if err != nil && !strings.Contains(string(output), "already exists") {
		t.Fatalf("create bucket failed: %v output: %s", err, string(output))
	}
}

func writeS3Config(t *testing.T, dbPath, replicaURL, endpoint string) string {
	t.Helper()
	configPath := filepath.Join(filepath.Dir(dbPath), "litestream-s3-drop.yml")
	config := fmt.Sprintf(`access-key-id: minioadmin
secret-access-key: minioadmin

dbs:
  - path: %s
    snapshot:
      interval: 1s
      retention: 1h
    replicas:
      - url: %s
        endpoint: %s
        region: us-east-1
        force-path-style: true
        skip-verify: true
        sync-interval: 1s
`, filepath.ToSlash(dbPath), replicaURL, endpoint)

	if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	return configPath
}

func insertLargeRows(dbPath string, rows int, blobSize int) error {
	sqlDB, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}
	defer sqlDB.Close()

	if _, err := sqlDB.Exec(`CREATE TABLE IF NOT EXISTS drop_test(id INTEGER PRIMARY KEY, data BLOB);`); err != nil {
		return err
	}

	for i := 0; i < rows; i++ {
		if _, err := sqlDB.Exec(`INSERT INTO drop_test(data) VALUES (randomblob(?));`, blobSize); err != nil {
			return err
		}
	}

	return nil
}

func verifyRestoredRowCount(dbPath string, expected int) error {
	sqlDB, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}
	defer sqlDB.Close()

	var count int
	if err := sqlDB.QueryRow(`SELECT COUNT(*) FROM drop_test;`).Scan(&count); err != nil {
		return err
	}
	if count != expected {
		return fmt.Errorf("restored row count: got %d want %d", count, expected)
	}
	return nil
}

type toxiproxyClient struct {
	baseURL string
	client  *http.Client
}

func newToxiproxyClient(t *testing.T, baseURL string) *toxiproxyClient {
	t.Helper()
	return &toxiproxyClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

func (c *toxiproxyClient) createProxy(t *testing.T, name, listen, upstream string) {
	t.Helper()
	payload := map[string]string{
		"name":     name,
		"listen":   listen,
		"upstream": upstream,
	}
	c.postJSON(t, "/proxies", payload, http.StatusOK)
}

func (c *toxiproxyClient) addResetPeerToxic(t *testing.T, proxy, name string, timeoutMS int) {
	t.Helper()
	payload := map[string]interface{}{
		"name":     name,
		"type":     "reset_peer",
		"stream":   "downstream",
		"toxicity": 1.0,
		"attributes": map[string]int{
			"timeout": timeoutMS,
		},
	}
	c.postJSON(t, fmt.Sprintf("/proxies/%s/toxics", proxy), payload, http.StatusOK)
}

func (c *toxiproxyClient) removeToxic(t *testing.T, proxy, name string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete, c.baseURL+fmt.Sprintf("/proxies/%s/toxics/%s", proxy, name), nil)
	if err != nil {
		t.Fatalf("create delete request: %v", err)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		t.Fatalf("delete toxic: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("delete toxic failed: status=%d body=%s", resp.StatusCode, string(body))
	}
}

func (c *toxiproxyClient) postJSON(t *testing.T, path string, payload interface{}, expectedStatus int) {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		t.Fatalf("post %s: %v", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != expectedStatus {
		if expectedStatus == http.StatusOK && resp.StatusCode == http.StatusCreated {
			return
		}
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("post %s failed: status=%d body=%s", path, resp.StatusCode, string(respBody))
	}
}
