package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"gopkg.in/yaml.v3"
)

// WriteConfig atomically writes config to file.
// It uses a temp file + rename approach to ensure atomic writes.
func WriteConfig(config *Config, path string) error {
	data, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	return writeConfigData(path, data)
}

// WriteConfigEnabled updates the enabled flag for a database entry on disk.
// Returns true if the database entry was found and updated.
func WriteConfigEnabled(path, dbPath string, enabled bool) (bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return false, fmt.Errorf("read config file: %w", err)
	}

	var node yaml.Node
	if err := yaml.Unmarshal(data, &node); err != nil {
		return false, fmt.Errorf("unmarshal config: %w", err)
	}
	if len(node.Content) == 0 {
		return false, nil
	}

	root := node.Content[0]
	if root.Kind != yaml.MappingNode {
		return false, fmt.Errorf("invalid config format")
	}

	found := false
	for i := 0; i < len(root.Content); i += 2 {
		if root.Content[i].Value != "dbs" {
			continue
		}

		dbsNode := root.Content[i+1]
		if dbsNode.Kind != yaml.SequenceNode {
			return false, fmt.Errorf("dbs must be a list")
		}

		for _, dbNode := range dbsNode.Content {
			if dbNode.Kind != yaml.MappingNode {
				continue
			}

			var pathValue string
			for j := 0; j < len(dbNode.Content); j += 2 {
				if dbNode.Content[j].Value == "path" {
					pathValue = dbNode.Content[j+1].Value
					break
				}
			}
			if pathValue == "" {
				continue
			}

			expandedPath, err := expand(pathValue)
			if err != nil || expandedPath != dbPath {
				expandedPath, err = expand(os.ExpandEnv(pathValue))
				if err != nil || expandedPath != dbPath {
					continue
				}
			}

			found = true
			SetDBNodeEnabled(dbNode, enabled)
		}
	}

	if !found {
		return false, nil
	}

	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(&node); err != nil {
		_ = enc.Close()
		return false, fmt.Errorf("encode config: %w", err)
	}
	if err := enc.Close(); err != nil {
		return false, fmt.Errorf("encode config: %w", err)
	}

	return true, writeConfigData(path, buf.Bytes())
}

// WriteConfigAddDB adds a database entry to the config file.
func WriteConfigAddDB(path string, dbNode *yaml.Node) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config file: %w", err)
	}

	var node yaml.Node
	if err := yaml.Unmarshal(data, &node); err != nil {
		return fmt.Errorf("unmarshal config: %w", err)
	}
	if len(node.Content) == 0 {
		node.Content = []*yaml.Node{{Kind: yaml.MappingNode}}
	}

	root := node.Content[0]
	if root.Kind != yaml.MappingNode {
		return fmt.Errorf("invalid config format")
	}

	var dbsNode *yaml.Node
	for i := 0; i < len(root.Content); i += 2 {
		if root.Content[i].Value != "dbs" {
			continue
		}
		dbsNode = root.Content[i+1]
		break
	}
	if dbsNode == nil {
		dbsNode = &yaml.Node{Kind: yaml.SequenceNode}
		root.Content = append(root.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Value: "dbs"},
			dbsNode,
		)
	}
	if dbsNode.Kind != yaml.SequenceNode {
		return fmt.Errorf("dbs must be a list")
	}

	dbsNode.Content = append(dbsNode.Content, cloneNode(dbNode))

	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(&node); err != nil {
		_ = enc.Close()
		return fmt.Errorf("encode config: %w", err)
	}
	if err := enc.Close(); err != nil {
		return fmt.Errorf("encode config: %w", err)
	}

	return writeConfigData(path, buf.Bytes())
}

// LoadDBConfigNode returns the raw database node from a config file.
// Returns nil if not found.
func LoadDBConfigNode(path, dbPath string) (*yaml.Node, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	var node yaml.Node
	if err := yaml.Unmarshal(data, &node); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	if len(node.Content) == 0 {
		return nil, nil
	}

	root := node.Content[0]
	if root.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("invalid config format")
	}

	for i := 0; i < len(root.Content); i += 2 {
		if root.Content[i].Value != "dbs" {
			continue
		}

		dbsNode := root.Content[i+1]
		if dbsNode.Kind != yaml.SequenceNode {
			return nil, fmt.Errorf("dbs must be a list")
		}

		for _, dbNode := range dbsNode.Content {
			if dbNode.Kind != yaml.MappingNode {
				continue
			}

			pathValue := ""
			for j := 0; j < len(dbNode.Content); j += 2 {
				if dbNode.Content[j].Value == "path" {
					pathValue = dbNode.Content[j+1].Value
					break
				}
			}
			if pathValue == "" {
				continue
			}

			expandedPath, err := expand(pathValue)
			if err != nil || expandedPath != dbPath {
				expandedPath, err = expand(os.ExpandEnv(pathValue))
				if err != nil || expandedPath != dbPath {
					continue
				}
			}

			return cloneNode(dbNode), nil
		}
	}

	return nil, nil
}

// SetDBNodeEnabled updates or inserts the enabled field on a DB config node.
func SetDBNodeEnabled(dbNode *yaml.Node, enabled bool) {
	if dbNode == nil {
		return
	}
	updated := false
	for j := 0; j < len(dbNode.Content); j += 2 {
		if dbNode.Content[j].Value == "enabled" {
			dbNode.Content[j+1].Kind = yaml.ScalarNode
			dbNode.Content[j+1].Tag = "!!bool"
			dbNode.Content[j+1].Value = strconv.FormatBool(enabled)
			updated = true
			break
		}
	}
	if !updated {
		dbNode.Content = append(dbNode.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Value: "enabled"},
			&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: strconv.FormatBool(enabled)},
		)
	}
}

func cloneNode(n *yaml.Node) *yaml.Node {
	if n == nil {
		return nil
	}
	clone := *n
	if len(n.Content) > 0 {
		clone.Content = make([]*yaml.Node, len(n.Content))
		for i, child := range n.Content {
			clone.Content[i] = cloneNode(child)
		}
	}
	return &clone
}

func writeConfigData(path string, data []byte) error {
	info, err := os.Stat(path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("stat config file: %w", err)
	}
	perm := os.FileMode(0600)
	if info != nil {
		perm = info.Mode()
	}

	dir := filepath.Dir(path)
	tmpFile, err := os.CreateTemp(dir, ".litestream-config-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		return fmt.Errorf("write temp file: %w", err)
	}

	if err := tmpFile.Chmod(perm); err != nil {
		tmpFile.Close()
		return fmt.Errorf("chmod temp file: %w", err)
	}

	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		return fmt.Errorf("sync temp file: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename temp file: %w", err)
	}

	return nil
}

// UpdateDBConfigInMemory finds and updates a DB config entry.
// Returns the updated config and true if found, nil and false otherwise.
func UpdateDBConfigInMemory(config *Config, dbPath string, updater func(*DBConfig)) (*DBConfig, bool) {
	for i := range config.DBs {
		expandedPath, err := expand(config.DBs[i].Path)
		if err != nil {
			continue
		}
		if expandedPath == dbPath {
			updater(config.DBs[i])
			return config.DBs[i], true
		}
	}
	return nil, false
}
