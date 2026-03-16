package cluster

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config represents cluster configuration
type Config struct {
	Cluster ClusterConfig `yaml:"cluster"`
}

type ClusterConfig struct {
	Nodes []NodeConfig `yaml:"nodes"`
}

type NodeConfig struct {
	ID         string `yaml:"id"`
	Address    string `yaml:"address"`
	ClientPort string `yaml:"client_port"`
	DataDir    string `yaml:"data_dir"`
}

// LoadConfig loads cluster configuration from file
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	return &config, nil
}

// GetNode returns config for a specific node by ID
func (c *Config) GetNode(id string) (*NodeConfig, error) {
	for _, node := range c.Cluster.Nodes {
		if node.ID == id {
			return &node, nil
		}
	}
	return nil, fmt.Errorf("node %s not found in config", id)
}

// GetPeers returns all nodes except the given one
func (c *Config) GetPeers(myID string) []NodeConfig {
	var peers []NodeConfig
	for _, node := range c.Cluster.Nodes {
		if node.ID != myID {
			peers = append(peers, node)
		}
	}
	return peers
}
