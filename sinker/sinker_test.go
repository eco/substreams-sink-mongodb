package sinker

import (
	"context"
	"testing"

	"github.com/streamingfast/bstream"
	pbdatabase "github.com/streamingfast/substreams-sink-mongodb/pb/substreams/sink/database/v1"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	"github.com/streamingfast/substreams-sink-mongodb/mongo"
	"github.com/stretchr/testify/require"
)

func TestHandleBlockScopedData_NilGuards(t *testing.T) {
	s := &MongoSinker{}
	ctx := context.Background()

	// Test nil data
	err := s.HandleBlockScopedData(ctx, nil, nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "received nil block scoped data")

	// Test nil output
	err = s.HandleBlockScopedData(ctx, &pbsubstreamsrpc.BlockScopedData{
		Output: nil,
	}, nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "received block scoped data without an output module")

	// Test nil clock
	err = s.HandleBlockScopedData(ctx, &pbsubstreamsrpc.BlockScopedData{
		Output: &pbsubstreamsrpc.MapModuleOutput{
			Name: "test_module",
		},
		Clock: nil,
	}, nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "received block scoped data without a clock")
}

func TestApplyDatabaseChanges_NullFieldMismatch(t *testing.T) {
	s := &MongoSinker{
		tables: mongo.Tables{
			"test_table": {
				"null_field": mongo.NULL,
			},
		},
	}

	changes := &pbdatabase.DatabaseChanges{
		TableChanges: []*pbdatabase.TableChange{
			{
				Table:     "test_table",
				Pk:        "1",
				Operation: pbdatabase.TableChange_CREATE,
				Fields: []*pbdatabase.Field{
					{
						Name:     "null_field",
						NewValue: "non-empty-value",
					},
				},
			},
		},
	}

	err := s.applyDatabaseChanges(context.Background(), bstream.BlockRefEmpty, changes)
	require.Error(t, err)
	require.Contains(t, err.Error(), "is typed as null but carries value")
}

func TestApplyDatabaseChanges_DateParsing(t *testing.T) {
	s := &MongoSinker{
		tables: mongo.Tables{
			"test_table": {
				"date_field": mongo.DATE,
			},
		},
	}

	t.Run("malformed date string", func(t *testing.T) {
		changes := &pbdatabase.DatabaseChanges{
			TableChanges: []*pbdatabase.TableChange{
				{
					Table:     "test_table",
					Pk:        "1",
					Operation: pbdatabase.TableChange_CREATE,
					Fields: []*pbdatabase.Field{
						{
							Name:     "date_field",
							NewValue: "not-a-valid-date",
						},
					},
				},
			},
		}

		err := s.applyDatabaseChanges(context.Background(), bstream.BlockRefEmpty, changes)
		require.Error(t, err)
		// The error should come directly from the time.Parse function
		require.Contains(t, err.Error(), "parsing time")
	})

	t.Run("valid RFC3339 date string", func(t *testing.T) {
		changes := &pbdatabase.DatabaseChanges{
			TableChanges: []*pbdatabase.TableChange{
				{
					Table:     "test_table",
					Pk:        "2",
					Operation: pbdatabase.TableChange_CREATE,
					Fields: []*pbdatabase.Field{
						{
							Name:     "date_field",
							NewValue: "2023-01-01T15:04:05Z",
						},
					},
				},
			},
		}

		// When a valid date is parsed, the code will proceed to the next stage: s.loader.Save().
		// Since we haven't initialized a fully mocked Loader in the test environment, this will cause a nil panic.
		// By catching the panic, we verify that the "time.Parse" step was passed successfully without errors.
		require.Panics(t, func() {
			_ = s.applyDatabaseChanges(context.Background(), bstream.BlockRefEmpty, changes)
		})
	})
}
