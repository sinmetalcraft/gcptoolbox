package tables

type apiOptions struct {
	overwriteExpiration bool
	dryRun              bool
	baseDate            BaseDate
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

// WithBaseDate is Table Expiration計算時の基準となる日をどれにするのか
func WithBaseDate(baseDate BaseDate) APIOptions {
	return func(ops *apiOptions) {
		ops.baseDate = baseDate
	}
}
