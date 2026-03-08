package detector

import "strings"

func IsAITriggered(content string, directAIChat bool) bool {
	return directAIChat || strings.Contains(content, "@AI")
}
