package pipeline_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"

	"github.com/avinash-gupta-rdz/rdstail/internal/config"
	"github.com/avinash-gupta-rdz/rdstail/internal/pipeline"
	"github.com/avinash-gupta-rdz/rdstail/internal/sink/memory"
	filestore "github.com/avinash-gupta-rdz/rdstail/internal/state/file"
)

func TestScheduler_RunsMultipleInstances_AndShutsDown(t *testing.T) {
	const fname = "error/postgresql.log"
	makeAPI := func() *scriptedAPI {
		return &scriptedAPI{
			describeResponses: []*awsrds.DescribeDBLogFilesOutput{
				{DescribeDBLogFiles: []rdstypes.DescribeDBLogFilesDetails{
					{LogFileName: aws.String(fname), Size: aws.Int64(100), LastWritten: aws.Int64(1)},
				}},
			},
			downloadByKey: map[string]*awsrds.DownloadDBLogFilePortionOutput{
				fname + "|0": {LogFileData: aws.String("line1\nline2\n"), Marker: aws.String("end"), AdditionalDataPending: aws.Bool(false)},
			},
		}
	}

	cfg := &config.Config{
		Runtime: config.Runtime{
			PollInterval:           20 * time.Millisecond,
			MaxWorkers:             4,
			MaxInstancesConcurrent: 2,
			ShutdownTimeout:        500 * time.Millisecond,
			StartFrom:              config.StartFromBeginning,
		},
	}
	sink := memory.New("mem")
	p := filepath.Join(t.TempDir(), "state.json")
	store, err := filestore.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	api1, api2 := makeAPI(), makeAPI()
	sched, err := pipeline.NewScheduler(pipeline.SchedulerOpts{
		Config: cfg,
		Store:  store,
		Sink:   sink,
		Instances: []pipeline.InstanceSpec{
			{InstanceID: "db-1", Engine: "postgres", API: api1},
			{InstanceID: "db-2", Engine: "postgres", API: api2},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	if err := sched.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}

	// Each instance delivered at least once (2 records per poll).
	if sink.RecordCount() < 4 {
		t.Fatalf("expected >=4 records across both instances, got %d", sink.RecordCount())
	}

	got1, _, _ := store.Get(context.Background(), "db-1", fname)
	got2, _, _ := store.Get(context.Background(), "db-2", fname)
	if got1.Marker != "end" || got2.Marker != "end" {
		t.Fatalf("expected both to checkpoint at 'end': %+v %+v", got1, got2)
	}
}

func TestScheduler_NoInstances_IdlesUntilCancel(t *testing.T) {
	cfg := &config.Config{Runtime: config.Runtime{ShutdownTimeout: 50 * time.Millisecond}}
	sink := memory.New("mem")
	store, _ := filestore.Open(filepath.Join(t.TempDir(), "s.json"))
	defer store.Close()
	sched, err := pipeline.NewScheduler(pipeline.SchedulerOpts{
		Config: cfg, Store: store, Sink: sink,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := sched.Run(ctx); err != nil {
		t.Fatalf("expected clean exit, got %v", err)
	}
}
