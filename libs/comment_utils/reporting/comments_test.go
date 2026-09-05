package reporting

import (
	"strings"
	"testing"
)

func TestCommentFormattingPreservesPercentCharacters(t *testing.T) {
	const title = "modules/100%/%s"
	const body = "output: %v and 25%"
	for name, formatter := range map[string]func(string) string{
		"plain":                 AsComment(title),
		"collapsible":           AsCollapsibleComment(title, false),
		"terraform":             GetTerraformOutputAsComment(title),
		"collapsible terraform": GetTerraformOutputAsCollapsibleComment(title, true),
	} {
		t.Run(name, func(t *testing.T) {
			got := formatter(body)
			if !strings.Contains(got, title) || !strings.Contains(got, body) {
				t.Fatalf("formatter changed literal content: %q", got)
			}
		})
	}
}
