package tables

type apiOptions struct {
	overwriteExpiration bool
	dryRun              bool
}

type APIOptions func(options *apiOptions)

// WithOverwriteExpiration is TableにExpirationがあっても上書きする
func WithOverwriteExpiration() APIOptions {
	return func(ops *apiOptions) {
		ops.overwriteExpiration = true
	}
}

// WithDryRun is 実際には実行しない
func WithDryRun() APIOptions {
	return func(ops *apiOptions) {
		ops.dryRun = true
	}
}
