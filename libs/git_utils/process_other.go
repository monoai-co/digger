//go:build !unix

package git_utils

import "os/exec"

// Non-Unix clients retain os/exec's default cancellation behavior. The backend
// comparison reader runs on Unix, where transport children are cancelled too.
func configureGitCommandCancellation(command *exec.Cmd) {}
