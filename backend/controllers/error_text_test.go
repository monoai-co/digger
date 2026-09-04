package controllers

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

func TestDurableWorkerErrorsRemainValidUTF8WhenTruncated(t *testing.T) {
	message := strings.Repeat("a", 16*1024-1) + "€" + "tail"
	for name, truncate := range map[string]func(string) string{
		"outbox":  truncateOutboxError,
		"webhook": truncateWebhookError,
	} {
		t.Run(name, func(t *testing.T) {
			truncated := truncate(message)
			require.LessOrEqual(t, len(truncated), 16*1024)
			require.True(t, utf8.ValidString(truncated))
			require.Equal(t, strings.Repeat("a", 16*1024-1), truncated)
		})
	}
}

func TestDurableWorkerErrorsReplaceInvalidUTF8(t *testing.T) {
	truncated := truncateValidUTF8("provider: "+string([]byte{0xff})+" failure", 16*1024)
	require.True(t, utf8.ValidString(truncated))
	require.Equal(t, "provider: � failure", truncated)
}
