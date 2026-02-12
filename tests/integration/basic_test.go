//go:build integration

package integration

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBinariesExist(t *testing.T) {
	// Navigate to project root (tests/integration -> tests -> project root)
	projectRoot := filepath.Join("..", "..")

	binaries := []string{
		"bin/forgekv",
		"bin/forgekvctl",
		"bin/forgekvbench",
	}

	for _, binary := range binaries {
		path := filepath.Join(projectRoot, binary)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("binary %s does not exist", binary)
		}
	}
}
