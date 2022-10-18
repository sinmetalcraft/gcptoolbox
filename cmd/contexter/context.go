package contexter

import "context"

type ctxKey int

const (
	notFoundKey ctxKey = iota
	projectIDKey
)

func WithProjectID(ctx context.Context, projectID string) context.Context {
	return context.WithValue(ctx, projectIDKey, projectID)
}

func ProjectID(ctx context.Context) (string, bool) {
	v, ok := ctx.Value(projectIDKey).(string)
	return v, ok
}
