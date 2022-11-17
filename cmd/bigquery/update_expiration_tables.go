package bigquery

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/apstndb/adcplus/tokensource"
	"github.com/sinmetalcraft/gcptoolbox/cmd/contexter"
	"github.com/spf13/cobra"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func cmdUpdateExpirationTables() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "update-expiration-tables [dataset duration]",
		Short:   "Update expiration of table in specified dataset",
		Long:    "Update expiration of table in specified dataset",
		Example: "gcptoolbox bq --project hoge update-expiration-tables public-dataset 365d",
		Args:    cobra.MatchAll(cobra.ExactArgs(2), cobra.OnlyValidArgs),
		RunE:    runUpdateExpirationTables,
	}
	cmd.Flags().StringVar(&prefix, "prefix", "", "table prefix")
	return cmd
}

func runUpdateExpirationTables(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	projectID, ok := contexter.ProjectID(ctx)
	if !ok {
		return fmt.Errorf("project required")
	}

	ts, err := tokensource.SmartAccessTokenSource(ctx)
	if err != nil {
		return err
	}

	bq, err := bigquery.NewClient(ctx, projectID, option.WithTokenSource(ts))
	if err != nil {
		return err
	}
	defer func() {
		if err := bq.Close(); err != nil {
			// noop
		}
	}()

	//var ops []bqbox.APIOptions
	datasetID = args[0]
	fmt.Println("bigquery update expiration")
	fmt.Printf("ProjectID=%s\n", projectID)
	fmt.Printf("DatasetID=%s\n", datasetID)
	fmt.Printf("TablePrefix=%s\n", prefix)
	ep := args[1]
	expiration, err := parseExpirationParam(ep)
	if err != nil {
		return fmt.Errorf("%s is invalid duration format: %w", ep, err)
	}

	fmt.Printf("Expiration=%s\n", expiration)
	// fmt.Printf("DryRun=%t\n", dryRun)
	//if dryRun {
	//	ops = append(ops, bqbox.WithDryRun())
	//}
	fmt.Println()
	iter := bq.DatasetInProject(projectID, datasetID).Tables(ctx)
	for {
		t, err := iter.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return err
		}

		if len(prefix) > 0 {
			if !strings.HasPrefix(t.TableID, prefix) {
				fmt.Printf("%s is has not prefix\n", t.TableID)
				continue
			}
		}

		tm, err := t.Metadata(ctx)
		var gapiErr *googleapi.Error
		if errors.As(err, &gapiErr) {
			if gapiErr.Code == http.StatusNotFound {
				fmt.Printf("%s is not found\n", t.TableID)
				continue
			} else {
				return err
			}
		} else if err != nil {
			return err
		}

		// TimePartitioningの場合
		if tm.TimePartitioning != nil {
			if tm.TimePartitioning.Expiration != 0 {
				fmt.Printf("%s is exist partitioning expiration duration. %s\n", t.TableID, tm.TimePartitioning.Expiration)
				continue
			}
			_, err = t.Update(ctx, bigquery.TableMetadataToUpdate{
				TimePartitioning: &bigquery.TimePartitioning{
					Expiration: expiration.Duration(),
				},
			}, tm.ETag)
			if errors.As(err, &gapiErr) {
				if gapiErr.Code == http.StatusNotFound {
					fmt.Printf("%s is not found\n", t.TableID)
					continue
				} else {
					return err
				}
			} else if err != nil {
				return err
			}
			fmt.Printf("%s set partitioning expiration\n", t.TableID)
			continue
		}

		// Sharding Table等の場合
		if !tm.ExpirationTime.IsZero() && !expiration.isNever { // NeverはExpirationTimeが設定されているものを上書きするために使うはずなので、Neverがtrueの場合は先に進む
			fmt.Printf("%s is exist expiration time. %s\n", t.TableID, tm.ExpirationTime)
			continue
		}

		_, err = t.Update(ctx, bigquery.TableMetadataToUpdate{
			ExpirationTime: expiration.ExpirationTime(tm.CreationTime),
		}, tm.ETag)
		if errors.As(err, &gapiErr) {
			if gapiErr.Code == http.StatusNotFound {
				fmt.Printf("%s is not found\n", t.TableID)
				continue
			} else {
				return err
			}
		} else if err != nil {
			return err
		}
		fmt.Printf("%s set table expiration\n", t.TableID)
	}
	fmt.Println()
	fmt.Println("Done")
	return nil
}

type ExpirationParam struct {
	duration time.Duration
	isNever  bool
}

func (p *ExpirationParam) String() string {
	if p.isNever {
		return "Never"
	}
	return p.Duration().String()
}

func (p *ExpirationParam) Duration() time.Duration {
	if p.isNever {
		return 0
	}
	return p.duration
}

func (p *ExpirationParam) ExpirationTime(baseTime time.Time) time.Time {
	if p.isNever {
		return bigquery.NeverExpire
	}
	return baseTime.Add(p.duration)
}

func parseExpirationParam(v string) (*ExpirationParam, error) {
	if strings.ToLower(v) == "never" {
		return &ExpirationParam{
			isNever: true,
		}, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return nil, fmt.Errorf("%s is invalid duration format: %w", v, err)
	}
	return &ExpirationParam{
		duration: d,
	}, nil
}
