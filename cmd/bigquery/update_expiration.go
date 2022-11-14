package bigquery

import (
	"context"
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

func cmdUpdateExpiration() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update-tables-expiration",
		Short: "update-tables-expiration",
		RunE:  runUpdateExpiration,
	}
	cmd.Flags().StringVar(&datasetID, "dataset", "sampledataset", "dataset")
	cmd.Flags().StringVar(&prefix, "prefix", "", "prefix")
	return cmd
}

func runUpdateExpiration(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
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
	fmt.Println("bigquery update expiration")
	fmt.Printf("ProjectID=%s\n", projectID)
	fmt.Printf("DatasetID=%s\n", datasetID)
	fmt.Printf("TablePrefix=%s\n", prefix)
	expiration, err := time.ParseDuration(args[0])
	if err != nil {
		return fmt.Errorf("%s is invalid duration format: %w", args[0], err)
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
					Expiration: expiration,
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
		if !tm.ExpirationTime.IsZero() {
			fmt.Printf("%s is exist expiration time. %s\n", t.TableID, tm.ExpirationTime)
			continue
		}

		_, err = t.Update(ctx, bigquery.TableMetadataToUpdate{
			ExpirationTime: tm.CreationTime.Add(expiration),
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
