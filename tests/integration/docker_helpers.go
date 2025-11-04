//go:build integration

package integration

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func RequireDocker(t *testing.T) {
	t.Helper()
	if err := exec.Command("docker", "version").Run(); err != nil {
		t.Skip("Docker is not available, skipping test")
	}
}

func StartMinioTestContainer(t *testing.T) (string, string) {
	t.Helper()

	name := fmt.Sprintf("litestream-minio-%d", time.Now().UnixNano())
	exec.Command("docker", "rm", "-f", name).Run()

	args := []string{
		"run", "-d",
		"--name", name,
		"-p", "0:9000",
		"-e", "MINIO_ROOT_USER=minioadmin",
		"-e", "MINIO_ROOT_PASSWORD=minioadmin",
		"-e", "MINIO_DOMAIN=s3-accesspoint.127.0.0.1.nip.io",
		"minio/minio", "server", "/data",
	}
	containerID := runDockerCommand(t, args...)
	portInfo := runDockerCommand(t, "port", name, "9000/tcp")
	hostPort := parseDockerPort(t, portInfo)

	time.Sleep(5 * time.Second)

	t.Logf("Started MinIO container %s (%s) on port %s", name, containerID[:12], hostPort)
	return name, fmt.Sprintf("http://localhost:%s", hostPort)
}

func StopMinioTestContainer(t *testing.T, name string) {
	t.Helper()
	if name == "" {
		return
	}
	if os.Getenv("SOAK_KEEP_TEMP") != "" {
		t.Logf("SOAK_KEEP_TEMP set, preserving MinIO container: %s", name)
		return
	}
	exec.Command("docker", "rm", "-f", name).Run()
}

func runDockerCommand(t *testing.T, args ...string) string {
	t.Helper()
	cmd := exec.Command("docker", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s failed: %v\nOutput: %s", strings.Join(args, " "), err, string(output))
	}
	return strings.TrimSpace(string(output))
}

func parseDockerPort(t *testing.T, portInfo string) string {
	t.Helper()
	idx := strings.LastIndex(portInfo, ":")
	if idx == -1 || idx == len(portInfo)-1 {
		t.Fatalf("unexpected docker port output: %s", portInfo)
	}
	return portInfo[idx+1:]
}
