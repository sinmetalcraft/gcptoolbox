package executioner

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	dbadminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"google.golang.org/api/iterator"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

// Config holds the configuration options.
type Config struct {
	// DryRun is 実際にはDBの削除は行わず、ログ出力だけとなる
	DryRun bool

	// StartTime is metricsをチェックする期間の開始時刻
	StartTime time.Time

	// EndTime is metricsをチェックする期間の終了時刻
	EndTime time.Time

	// ExcludeInstances is 対象外とするInstanceたち
	ExcludeInstances []string

	// ExcludedDatabases is 対象外とするDBたち
	ExcludedDatabases []string
}

// Option is a function that modifies the Config.
type Option func(*Config)

// WithDryRun sets the DryRun option.
func WithDryRun(dryRun bool) Option {
	return func(cfg *Config) {
		cfg.DryRun = dryRun
	}
}

// WithStartTime sets the StartTime option.
func WithStartTime(startTime time.Time) Option {
	return func(cfg *Config) {
		cfg.StartTime = startTime
	}
}

// WithEndTime sets the EndTime option.
func WithEndTime(endTime time.Time) Option {
	return func(cfg *Config) {
		cfg.EndTime = endTime
	}
}

type Executioner struct {
	monitoringMetricCli *monitoring.MetricClient
	dbAdminCli          *database.DatabaseAdminClient
}

func NewExecutioner(ctx context.Context, monitoringMetricCli *monitoring.MetricClient, dbAdminCli *database.DatabaseAdminClient) *Executioner {
	return &Executioner{
		monitoringMetricCli: monitoringMetricCli,
		dbAdminCli:          dbAdminCli,
	}
}

// RunAllDatabases is 指定したSpanner InstanceのすべてのDBに実行する
func (e *Executioner) RunAllDatabases(ctx context.Context, projectID string, instance string, options ...Option) error {
	return nil
}

func (e *Executioner) Run(ctx context.Context, projectID string, instance string, database string, options ...Option) error {
	now := time.Now()
	cfg := &Config{
		DryRun:    false,
		StartTime: now.Add(-90 * 24 * time.Hour),
		EndTime:   now.Add(-1 * 24 * time.Hour),
	}

	for _, option := range options {
		option(cfg)
	}

	if cfg.StartTime.Sub(cfg.EndTime) >= 7*24*time.Hour {
		return fmt.Errorf("StartTime and EndTime must be at least 7 days apart. StartTime: %s, EndTime: %s", cfg.StartTime, cfg.EndTime)
	}

	execution, countActiveAPIRequest, err := e.IsExecution(ctx, projectID, instance, database, cfg.StartTime, cfg.EndTime, 1)
	if err != nil {
		return err
	}
	for k, v := range countActiveAPIRequest {
		fmt.Printf("%s:%d\n", k, v)
	}
	if execution {
		fmt.Printf("Execute %s\n", database)
		if cfg.DryRun {
			return nil
		}
		// TODO DB Delete
	} else {
		fmt.Printf("Let %s go\n", database)
	}

	return nil
}

// CountActiveAPIRequests is DBが使われている形跡があるAPI Requestをmethodごとにカウントする
func (e *Executioner) CountActiveAPIRequests(ctx context.Context, projectID string, instance string, database string, startTime time.Time, endTime time.Time) (map[string]int64, error) {
	result := make(map[string]int64)
	filter := fmt.Sprintf(`metric.type = "spanner.googleapis.com/api/api_request_count" AND resource.labels.instance_id = "%s" AND metric.labels.database = "%s"`, instance, database)

	iter := e.monitoringMetricCli.ListTimeSeries(ctx, &monitoringpb.ListTimeSeriesRequest{
		Name:   fmt.Sprintf("projects/%s", projectID),
		Filter: filter,
		Interval: &monitoringpb.TimeInterval{
			EndTime:   timestamppb.New(endTime),
			StartTime: timestamppb.New(startTime),
		},
		Aggregation: &monitoringpb.Aggregation{
			AlignmentPeriod:    nil,
			PerSeriesAligner:   0,
			CrossSeriesReducer: 0,
			GroupByFields:      []string{"method"},
		},
		SecondaryAggregation: nil,
		OrderBy:              "",
		View:                 0,
		PageSize:             0,
		PageToken:            "",
	})
	for {
		timeSeries, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}

		var method string
		for k, v := range timeSeries.GetMetric().GetLabels() {
			switch k {
			case "database":
				// np
			case "method":
				method = v
			default:
				// np
			}
		}

		// AutoScalerなどのToolが実行するmethodはスルーしている
		switch method {
		case "CreateDatabase":
		case "CreateSession":
		case "BatchCreateSessions":
		case "DropDatabase":
			continue
		case "ExecuteSql", "ExecuteStreamingSql", "StreamingRead", "BeginTransaction", "Commit", "Rollback", "PartitionQuery":
			// noop
		case "GetOperation":
			continue
		case "GetDatabase":
			continue
		case "GetDatabaseDdl":
			continue
		case "ListBackupSchedules":
			continue
		default:
			fmt.Printf("method:%s\n", method)
		}

		for _, point := range timeSeries.GetPoints() {
			//fmt.Printf("Unit:%v\n", timeSeries.GetUnit())
			//fmt.Printf("ValueType:%v\n", timeSeries.GetValueType())
			//fmt.Printf("Metadata:%v\n", timeSeries.GetMetadata())
			//fmt.Printf("MetricKind:%v\n", timeSeries.GetMetricKind())
			//fmt.Printf("Metric:%v\n", timeSeries.GetMetric())
			//fmt.Println(timeSeries.GetResource().String())
			count, ok := result[method]
			if !ok {
				count = 0
			}
			count += point.GetValue().GetInt64Value()
			result[method] = count

			// fmt.Printf("%v:%v:%s:%s %s %d\n", timeSeries.GetValueType(), timeSeries.GetResource(), database, method, point.GetInterval().StartTime.AsTime(), point.GetValue().GetInt64Value())
		}
	}
	return result, nil
}

// IsExecution is 削除対象にするかどうかの判定
//
// 利用されている形跡がない場合、削除対象にする
func (e *Executioner) IsExecution(ctx context.Context, projectID string, instance string, database string, startTime time.Time, endTime time.Time, survivalThreshold int) (isExecution bool, countActiveAPIRequests map[string]int64, err error) {
	countActiveAPIRequests, err = e.CountActiveAPIRequests(ctx, projectID, instance, database, startTime, endTime)
	if err != nil {
		return false, nil, err
	}

	_, ok := countActiveAPIRequests["CreateDatabase"]
	if ok {
		// 期間内に作成されたDBは維持
		return false, countActiveAPIRequests, nil
	}

	survivalMethods := []string{
		"ExecuteSql",
		"ExecuteStreamingSql",
		"PartitionQuery",
		"StreamingRead",
		"Rollback",
		"BatchCreateSessions",
		"BeginTransaction",
		"Commit",
	}
	for _, method := range survivalMethods {
		v, ok := countActiveAPIRequests[method]
		if !ok {
			continue
		}
		if v >= int64(survivalThreshold) {
			return false, countActiveAPIRequests, nil
		}
	}

	return true, countActiveAPIRequests, nil
}

// CreateBackup is 対象のDBがExecution対象の場合、SpannerのBackupを作成する
// Expireは364日後に固定にしているが、深い意味はない
func (e *Executioner) CreateBackup(ctx context.Context, projectID string, instanceID string, databaseID string, options ...Option) (ope *database.CreateBackupOperation, isExecution bool, err error) {
	now := time.Now()
	cfg := &Config{
		DryRun:    false,
		StartTime: now.Add(-90 * 24 * time.Hour),
		EndTime:   now.Add(-1 * 24 * time.Hour),
	}

	for _, option := range options {
		option(cfg)
	}

	if cfg.StartTime.Sub(cfg.EndTime) >= 7*24*time.Hour {
		return nil, false, fmt.Errorf("StartTime and EndTime must be at least 7 days apart. StartTime: %s, EndTime: %s", cfg.StartTime, cfg.EndTime)
	}

	execution, _, err := e.IsExecution(ctx, projectID, instanceID, databaseID, cfg.StartTime, cfg.EndTime, 1)
	if err != nil {
		return nil, false, err
	}
	if !execution {
		return nil, false, nil
	}

	backupID := fmt.Sprintf("%s-%s", databaseID, time.Now().Format("20060102"))
	req := &dbadminpb.CreateBackupRequest{
		Parent:   fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID),
		BackupId: backupID,
		Backup: &dbadminpb.Backup{
			Database:   fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID),
			ExpireTime: &timestamppb.Timestamp{Seconds: time.Now().Add(364 * 24 * time.Hour).Unix()},
		},
	}
	if cfg.DryRun {
		fmt.Printf("dry-run: creating backup %s\n", backupID)
		return nil, false, nil
	}
	ope, err = e.dbAdminCli.CreateBackup(ctx, req)
	if err != nil {
		return nil, false, err
	}
	return ope, true, nil
}

func (e *Executioner) DeleteDatabase(ctx context.Context, projectID string, instanceID string, databaseID string, backupOperationName string) (bool, error) {
	ope, err := e.dbAdminCli.GetOperation(ctx, &longrunningpb.GetOperationRequest{
		Name: backupOperationName,
	})
	if err != nil {
		return false, err
	}
	if !ope.Done {
		return false, nil
	}

	sts := ope.GetError()
	fmt.Printf("sts: %v\n", sts)

	// TODO Delete Database
	return true, nil
}

func (e *Executioner) ListDatabase(ctx context.Context, projectID string, instanceID string) ([]string, error) {
	dbNamePrefix := fmt.Sprintf("projects/%s/instances/%s/databases/", projectID, instanceID)
	var results []string
	iter := e.dbAdminCli.ListDatabases(ctx, &databasepb.ListDatabasesRequest{
		Parent:    fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID),
		PageSize:  0,
		PageToken: "",
	})
	for {
		db, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}
		results = append(results, strings.ReplaceAll(db.GetName(), dbNamePrefix, ""))
	}
	return results, nil
}
