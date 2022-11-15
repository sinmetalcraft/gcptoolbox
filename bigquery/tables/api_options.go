package tables

type apiOptions struct {
	overwriteExpiration bool
}

type APIOptions func(options *apiOptions)

// WithOverwriteExpiration is TableにExpirationがあっても上書きする
func WithOverwriteExpiration() APIOptions {
	return func(ops *apiOptions) {
		ops.overwriteExpiration = true
	}
}
