# Executioner

利用されていないSpanner DBのBackupを作成し、削除する。
MonitoringのAPI Callの回数をチェックして利用してないかを判定している。

## Config

### $GCPTOOLBOX_EXECUTIONER=true

### $GCPTOOLBOX_IAM_BACKUP_BUCKET=hoge-iam-backup

設定するとSpanner DBのIAMをJSON形式で出力する
