package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/apstndb/adcplus/tokensource"
	bqbox "github.com/sinmetalcraft/gcpbox/bigquery"
	"github.com/spf13/cobra"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var (
	BigQueryCmd *cobra.Command
	datasetID   string
	prefix      string
	dryRun      bool
)

func init() {
	BigQueryCmd = &cobra.Command{
		Use:   "bq",
		Short: "command line bigquery",
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("Command name argument expected.")
		},
	}
}

func bigQueryDeleteTablesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "deletetables",
		Short: "deletetables",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			ts, err := tokensource.SmartAccessTokenSource(ctx)
			if err != nil {
				return err
			}

			bq, err := bigquery.NewClient(ctx, projectID, option.WithTokenSource(ts))
			if err != nil {
				return err
			}
			bqboxService, err := bqbox.NewService(ctx, bq)
			if err != nil {
				return err
			}
			defer func() {
				if err := bqboxService.Close(ctx); err != nil {
					fmt.Printf("FIY: failed bqboxService.Close %s", err)
				}
			}()

			var ops []bqbox.APIOptions
			fmt.Println("bigquery delete tables")
			fmt.Printf("ProjectID=%s\n", projectID)
			fmt.Printf("DatasetID=%s\n", datasetID)
			fmt.Printf("TablePrefix=%s\n", args[0])
			fmt.Printf("DryRun=%t\n", dryRun)
			if dryRun {
				ops = append(ops, bqbox.WithDryRun())
			}
			fmt.Println()
			msgs, err := bqboxService.DeleteTablesByTablePrefix(ctx, projectID, datasetID, args[0], ops...)
			if err != nil {
				return err
			}
			for _, msg := range msgs {
				fmt.Println(msg)
			}
			fmt.Println()
			fmt.Println("Done")
			return nil
		},
	}
	cmd.Flags().StringVar(&datasetID, "dataset", "dataset", "dataset")
	cmd.Flags().BoolVar(&dryRun, "dryrun", false, "dryrun")
	return cmd
}

func bigQueryUpdateExpirationCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update_expiration",
		Short: "update_expiration",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			ts, err := tokensource.SmartAccessTokenSource(ctx)
			if err != nil {
				return err
			}

			bq, err := bigquery.NewClient(ctx, projectID, option.WithTokenSource(ts))
			if err != nil {
				return err
			}
			bqboxService, err := bqbox.NewService(ctx, bq)
			if err != nil {
				return err
			}
			defer func() {
				if err := bqboxService.Close(ctx); err != nil {
					fmt.Printf("FIY: failed bqboxService.Close %s", err)
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
		},
	}
	cmd.Flags().StringVar(&datasetID, "dataset", "sampledataset", "dataset")
	cmd.Flags().StringVar(&prefix, "prefix", "", "prefix")
	// cmd.Flags().BoolVar(&dryRun, "dryrun", false, "dryrun")
	return cmd
}
