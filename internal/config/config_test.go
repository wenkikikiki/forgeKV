package config

import (
	"testing"
)

func TestParsePeers(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    map[string]string
		wantErr bool
	}{
		{
			name:  "valid three peers",
			input: "n1=127.0.0.1:9101,n2=127.0.0.1:9102,n3=127.0.0.1:9103",
			want: map[string]string{
				"n1": "127.0.0.1:9101",
				"n2": "127.0.0.1:9102",
				"n3": "127.0.0.1:9103",
			},
			wantErr: false,
		},
		{
			name:    "empty string",
			input:   "",
			want:    map[string]string{},
			wantErr: false,
		},
		{
			name:    "invalid format no equals",
			input:   "n1127.0.0.1:9101",
			wantErr: true,
		},
		{
			name:  "with spaces",
			input: "n1 = 127.0.0.1:9101 , n2 = 127.0.0.1:9102",
			want: map[string]string{
				"n1": "127.0.0.1:9101",
				"n2": "127.0.0.1:9102",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParsePeers(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParsePeers() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if len(got) != len(tt.want) {
					t.Errorf("ParsePeers() got %d peers, want %d", len(got), len(tt.want))
					return
				}
				for k, v := range tt.want {
					if got[k] != v {
						t.Errorf("ParsePeers() got[%s] = %v, want %v", k, got[k], v)
					}
				}
			}
		})
	}
}

func TestParsePeerClientAddrs(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    map[string]string
		wantErr bool
	}{
		{
			name:  "valid three peers",
			input: "n1=127.0.0.1:9001,n2=127.0.0.1:9002,n3=127.0.0.1:9003",
			want: map[string]string{
				"n1": "127.0.0.1:9001",
				"n2": "127.0.0.1:9002",
				"n3": "127.0.0.1:9003",
			},
			wantErr: false,
		},
		{
			name:    "empty string",
			input:   "",
			want:    map[string]string{},
			wantErr: false,
		},
		{
			name:    "invalid format no equals",
			input:   "n1127.0.0.1:9001",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParsePeerClientAddrs(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParsePeerClientAddrs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if len(got) != len(tt.want) {
					t.Errorf("ParsePeerClientAddrs() got %d peers, want %d", len(got), len(tt.want))
					return
				}
				for k, v := range tt.want {
					if got[k] != v {
						t.Errorf("ParsePeerClientAddrs() got[%s] = %v, want %v", k, got[k], v)
					}
				}
			}
		})
	}
}

func TestNodeIDToRaftID(t *testing.T) {
	tests := []struct {
		nodeID string
		want   uint64
	}{
		{"n1", 1},
		{"n2", 2},
		{"n3", 3},
		{"custom", 0}, // Will be hash-based, just verify it doesn't panic
	}

	for _, tt := range tests {
		t.Run(tt.nodeID, func(t *testing.T) {
			got := NodeIDToRaftID(tt.nodeID)
			if tt.want != 0 && got != tt.want {
				t.Errorf("NodeIDToRaftID(%s) = %d, want %d", tt.nodeID, got, tt.want)
			}
			// For custom IDs, just verify it returns non-zero
			if tt.want == 0 && got == 0 {
				t.Errorf("NodeIDToRaftID(%s) = 0, want non-zero", tt.nodeID)
			}
		})
	}
}

func TestRaftIDToNodeID(t *testing.T) {
	tests := []struct {
		raftID uint64
		want   string
	}{
		{1, "n1"},
		{2, "n2"},
		{3, "n3"},
		{100, "node-100"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := RaftIDToNodeID(tt.raftID)
			if got != tt.want {
				t.Errorf("RaftIDToNodeID(%d) = %s, want %s", tt.raftID, got, tt.want)
			}
		})
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *Config
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: &Config{
				NodeID:     "n1",
				ClientAddr: "127.0.0.1:9001",
				RaftAddr:   "127.0.0.1:9101",
				DataDir:    "/tmp/forgekv/n1",
				Peers: map[string]string{
					"n1": "127.0.0.1:9101",
					"n2": "127.0.0.1:9102",
					"n3": "127.0.0.1:9103",
				},
				PeerClientAddrs: map[string]string{
					"n1": "127.0.0.1:9001",
					"n2": "127.0.0.1:9002",
					"n3": "127.0.0.1:9003",
				},
			},
			wantErr: false,
		},
		{
			name: "missing node ID",
			cfg: &Config{
				ClientAddr:      "127.0.0.1:9001",
				RaftAddr:        "127.0.0.1:9101",
				DataDir:         "/tmp/forgekv/n1",
				Peers:           map[string]string{"n1": "127.0.0.1:9101", "n2": "127.0.0.1:9102", "n3": "127.0.0.1:9103"},
				PeerClientAddrs: map[string]string{"n1": "127.0.0.1:9001", "n2": "127.0.0.1:9002", "n3": "127.0.0.1:9003"},
			},
			wantErr: true,
		},
		{
			name: "insufficient peers",
			cfg: &Config{
				NodeID:          "n1",
				ClientAddr:      "127.0.0.1:9001",
				RaftAddr:        "127.0.0.1:9101",
				DataDir:         "/tmp/forgekv/n1",
				Peers:           map[string]string{"n1": "127.0.0.1:9101", "n2": "127.0.0.1:9102"},
				PeerClientAddrs: map[string]string{"n1": "127.0.0.1:9001", "n2": "127.0.0.1:9002"},
			},
			wantErr: true,
		},
		{
			name: "self not in peers",
			cfg: &Config{
				NodeID:          "n1",
				ClientAddr:      "127.0.0.1:9001",
				RaftAddr:        "127.0.0.1:9101",
				DataDir:         "/tmp/forgekv/n1",
				Peers:           map[string]string{"n2": "127.0.0.1:9102", "n3": "127.0.0.1:9103", "n4": "127.0.0.1:9104"},
				PeerClientAddrs: map[string]string{"n2": "127.0.0.1:9002", "n3": "127.0.0.1:9003", "n4": "127.0.0.1:9004"},
			},
			wantErr: true,
		},
		{
			name: "missing peer client addrs",
			cfg: &Config{
				NodeID:          "n1",
				ClientAddr:      "127.0.0.1:9001",
				RaftAddr:        "127.0.0.1:9101",
				DataDir:         "/tmp/forgekv/n1",
				Peers:           map[string]string{"n1": "127.0.0.1:9101", "n2": "127.0.0.1:9102", "n3": "127.0.0.1:9103"},
				PeerClientAddrs: map[string]string{"n1": "127.0.0.1:9001", "n2": "127.0.0.1:9002"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
