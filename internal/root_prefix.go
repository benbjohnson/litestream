package internal

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/superfly/ltx"
)

// ListKeysFunc lists object keys matching prefix. When maxKeys > 0, it fetches
// at most maxKeys keys in a single request. Returning false from fn stops
// iteration early.
type ListKeysFunc func(ctx context.Context, prefix string, maxKeys int32, fn func(key string) bool) error

// IsRootLTXKey reports whether key is a Litestream LTX object directly under
// rootPrefix (e.g. "0000/<min>-<max>.ltx", "./0000/<min>-<max>.ltx", or
// "/0000/<min>-<max>.ltx") for a level in [0, maxLevel].
func IsRootLTXKey(rootPrefix, key string, maxLevel int) bool {
	if !strings.HasPrefix(key, rootPrefix) {
		return false
	}
	rest := strings.TrimPrefix(key, rootPrefix)
	levelStr, filename, ok := strings.Cut(rest, "/")
	if !ok || len(levelStr) != 4 || strings.Contains(filename, "/") {
		return false
	}
	level, err := strconv.ParseInt(levelStr, 16, 32)
	if err != nil || int(level) < 0 || int(level) > maxLevel || fmt.Sprintf("%04x", level) != levelStr {
		return false
	}
	_, _, err = ltx.ParseFilename(filename)
	return err == nil
}

// ResolveRootPrefix determines the object key prefix to use for a bare-bucket
// S3/OSS replica (configured with empty path "").
//
// New or empty buckets use the canonical clean root prefix "" ("0000/...").
// Existing replicas written by earlier versions under "./" ("./0000/...") or
// "/" ("/0000/...") are detected and preserved with a warning so upgrades do
// not orphan existing backup history. If multiple root layouts exist in the
// same bucket, an error is returned rather than silently choosing one.
// Callers with IAM policies restricted to "./*" can explicitly configure
// path: "." to bypass discovery and use "./" directly.
func ResolveRootPrefix(ctx context.Context, logger *slog.Logger, backend, bucket string, maxLevel int, listKeys ListKeysFunc) (string, error) {
	type candidate struct {
		rootPrefix      string
		fastProbePrefix string
		label           string
	}
	candidates := []candidate{
		{rootPrefix: "", fastProbePrefix: "000", label: "(root)"},
		{rootPrefix: "./", fastProbePrefix: "./", label: "./"},
		{rootPrefix: "/", fastProbePrefix: "/", label: "/"},
	}

	var found []candidate
	for _, cand := range candidates {
		exists, err := rootPrefixHasLTX(ctx, cand.rootPrefix, cand.fastProbePrefix, maxLevel, listKeys)
		if err != nil {
			return "", err
		}
		if exists {
			found = append(found, cand)
		}
	}

	switch len(found) {
	case 0:
		return "", nil
	case 1:
		if found[0].rootPrefix != "" && logger != nil {
			logger.Warn("legacy root replica prefix detected; continuing to use existing prefix",
				slog.String("bucket", bucket),
				slog.String("prefix", found[0].rootPrefix),
				slog.String("expected", ""))
		}
		return found[0].rootPrefix, nil
	default:
		labels := make([]string, len(found))
		for i, cand := range found {
			labels[i] = cand.label
		}
		return "", fmt.Errorf("%s: multiple root replica prefixes found in bucket %q (%s); migrate or remove unused prefixes to resolve ambiguity", backend, bucket, strings.Join(labels, ", "))
	}
}

func rootPrefixHasLTX(ctx context.Context, rootPrefix, fastProbePrefix string, maxLevel int, listKeys ListKeysFunc) (bool, error) {
	var sawAny, foundLTX bool
	if err := listKeys(ctx, fastProbePrefix, 1, func(key string) bool {
		sawAny = true
		if IsRootLTXKey(rootPrefix, key, maxLevel) {
			foundLTX = true
		}
		return false
	}); err != nil {
		return false, err
	}
	if !sawAny {
		return false, nil
	}
	if foundLTX {
		return true, nil
	}

	for level := 0; level <= maxLevel; level++ {
		levelPrefix := rootPrefix + fmt.Sprintf("%04x/", level)
		if err := listKeys(ctx, levelPrefix, 0, func(key string) bool {
			if IsRootLTXKey(rootPrefix, key, maxLevel) {
				foundLTX = true
				return false
			}
			return true
		}); err != nil {
			return false, err
		}
		if foundLTX {
			return true, nil
		}
	}
	return false, nil
}
