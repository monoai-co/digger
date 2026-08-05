package drift

import (
	"fmt"
	"os/exec"
	"strings"

	core_drift "github.com/diggerhq/digger/cli/pkg/core/drift"
)

// GetLastChange returns the most recent commit that touched projectPath.
// The checkout must have history (actions/checkout with fetch-depth: 0);
// on a shallow clone git may find no commit for the path, which is
// returned as an error and should be treated as non-fatal by callers.
func GetLastChange(projectPath string) (*core_drift.LastChange, error) {
	// %x1f is the ASCII unit separator: unlike "|" it cannot appear in
	// author names or emails, so fields always split cleanly.
	cmd := exec.Command("git", "log", "-1", "--format=%an%x1f%ae%x1f%h%x1f%ar", "--", ".")
	cmd.Dir = projectPath
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git log failed for %v: %v", projectPath, err)
	}
	line := strings.TrimSpace(string(out))
	parts := strings.SplitN(line, "\x1f", 4)
	if len(parts) < 4 || parts[0] == "" {
		return nil, fmt.Errorf("no git history found for %v", projectPath)
	}
	return &core_drift.LastChange{Author: parts[0], Email: parts[1], Commit: parts[2], When: parts[3]}, nil
}
