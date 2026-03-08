package llm

import "context"

type Client interface {
	Chat(ctx context.Context, memory string, input string) (string, error)
}
