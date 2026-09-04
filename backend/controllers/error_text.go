package controllers

import (
	"strings"
	"unicode/utf8"
)

func truncateValidUTF8(message string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	message = strings.ToValidUTF8(message, "\uFFFD")
	if len(message) <= maxBytes {
		return message
	}
	end := maxBytes
	for end > 0 && !utf8.ValidString(message[:end]) {
		end--
	}
	return message[:end]
}
