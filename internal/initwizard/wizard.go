// Package initwizard is the interactive `rdstail init` flow: list the
// region's RDS instances with ambient credentials, let the user pick
// instances and a sink, and emit a commented, validated rdstail.yaml. Pure
// prompt/render logic — AWS calls and file writes are injected, so the whole
// flow is unit-testable with scripted input.
package initwizard

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
)

// Instance is one selectable RDS instance.
type Instance struct {
	ID     string
	Engine string // postgres | mysql | mariadb
	Status string
}

// Deps are the wizard's inputs. In/Out drive the prompts; the listers use
// ambient AWS credentials and may fail — the wizard degrades to manual entry.
type Deps struct {
	In  io.Reader
	Out io.Writer
	// ListInstances returns the region's rdstail-supported instances.
	ListInstances func(ctx context.Context, region string) ([]Instance, error)
	// ListBuckets returns the account's S3 bucket names, resolved against the
	// prompted region's partition. nil or failing → free-text bucket entry.
	ListBuckets func(ctx context.Context, region string) ([]string, error)
	// FileExists guards against silently overwriting an existing config.
	FileExists func(path string) bool
}

// Result is the wizard's outcome: the YAML to write and where.
type Result struct {
	Path string
	YAML string
}

type wizard struct {
	Deps
	sc *bufio.Scanner
}

// Run walks the user through region → instances → sink → output path and
// returns the rendered YAML. It never writes files itself.
func Run(ctx context.Context, d Deps, defaultRegion, defaultPath string) (*Result, error) {
	w := &wizard{Deps: d, sc: bufio.NewScanner(d.In)}
	fmt.Fprintln(d.Out, "rdstail init — a working config in five questions. Enter accepts the [default].")
	fmt.Fprintln(d.Out)

	region, err := w.ask("AWS region", defaultRegion)
	if err != nil {
		return nil, err
	}
	if region == "" {
		return nil, fmt.Errorf("a region is required")
	}

	sources, err := w.pickInstances(ctx, region)
	if err != nil {
		return nil, err
	}
	sink, err := w.pickSink(ctx, region)
	if err != nil {
		return nil, err
	}

	path, err := w.ask("write config to", defaultPath)
	if err != nil {
		return nil, err
	}
	if w.FileExists != nil && w.FileExists(path) {
		ok, err := w.ask(fmt.Sprintf("%s exists — overwrite? (y/N)", path), "n")
		if err != nil {
			return nil, err
		}
		if a := strings.ToLower(ok); a != "y" && a != "yes" {
			return nil, fmt.Errorf("aborted: %s already exists", path)
		}
	}

	return &Result{Path: path, YAML: render(region, sources, sink)}, nil
}

// ask prompts once and returns the trimmed answer, or def when empty.
func (w *wizard) ask(prompt, def string) (string, error) {
	if def != "" {
		fmt.Fprintf(w.Out, "%s [%s]: ", prompt, def)
	} else {
		fmt.Fprintf(w.Out, "%s: ", prompt)
	}
	if !w.sc.Scan() {
		if err := w.sc.Err(); err != nil {
			return "", err
		}
		return "", fmt.Errorf("input closed before the wizard finished")
	}
	ans := strings.TrimSpace(w.sc.Text())
	if ans == "" {
		return def, nil
	}
	return ans, nil
}

// source is one engine-homogeneous group of instances.
type source struct {
	Engine    string
	Instances []string
}

// pickInstances lists the region and turns the user's selection into
// engine-grouped sources; discovery failure or an empty region degrades to
// manual entry.
func (w *wizard) pickInstances(ctx context.Context, region string) ([]source, error) {
	var found []Instance
	if w.ListInstances != nil {
		var err error
		found, err = w.ListInstances(ctx, region)
		if err != nil {
			fmt.Fprintf(w.Out, "  (could not list instances: %v — falling back to manual entry)\n", err)
		}
	}
	if len(found) == 0 {
		fmt.Fprintf(w.Out, "  no supported RDS instances visible in %s\n", region)
		return w.manualInstances()
	}

	fmt.Fprintf(w.Out, "\nRDS instances in %s:\n", region)
	for i, inst := range found {
		fmt.Fprintf(w.Out, "  %2d) %-40s %-9s %s\n", i+1, inst.ID, inst.Engine, inst.Status)
	}
	sel, err := w.ask("select instances (e.g. 1,3 | a = all | m = manual)", "a")
	if err != nil {
		return nil, err
	}
	switch strings.ToLower(sel) {
	case "m":
		return w.manualInstances()
	case "a", "all":
		return group(found), nil
	}
	var picked []Instance
	seen := map[int]bool{}
	for _, tok := range strings.Split(sel, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(tok))
		if err != nil || n < 1 || n > len(found) {
			return nil, fmt.Errorf("invalid selection %q: expected numbers 1-%d, 'a', or 'm'", tok, len(found))
		}
		if seen[n] {
			continue // "1,1" must not tail an instance twice
		}
		seen[n] = true
		picked = append(picked, found[n-1])
	}
	return group(picked), nil
}

func (w *wizard) manualInstances() ([]source, error) {
	ids, err := w.ask("instance identifiers (comma-separated)", "")
	if err != nil {
		return nil, err
	}
	var list []string
	for _, id := range strings.Split(ids, ",") {
		if id = strings.TrimSpace(id); id != "" {
			list = append(list, id)
		}
	}
	if len(list) == 0 {
		return nil, fmt.Errorf("at least one instance is required")
	}
	engine, err := w.ask("engine (postgres | mysql | mariadb)", "postgres")
	if err != nil {
		return nil, err
	}
	engine = strings.ToLower(engine)
	switch engine {
	case "postgres", "mysql", "mariadb":
	default:
		return nil, fmt.Errorf("unknown engine %q", engine)
	}
	return []source{{Engine: engine, Instances: list}}, nil
}

// group folds instances into one source per engine, both sorted for stable
// output.
func group(insts []Instance) []source {
	byEngine := map[string][]string{}
	for _, i := range insts {
		byEngine[i.Engine] = append(byEngine[i.Engine], i.ID)
	}
	engines := make([]string, 0, len(byEngine))
	for e := range byEngine {
		engines = append(engines, e)
	}
	sort.Strings(engines)
	out := make([]source, 0, len(engines))
	for _, e := range engines {
		ids := byEngine[e]
		sort.Strings(ids)
		out = append(out, source{Engine: e, Instances: ids})
	}
	return out
}

// sinkChoice carries everything render needs for the single chosen sink.
type sinkChoice struct {
	Type         string // s3 | kafka | http | stdout
	Bucket       string
	BucketRegion string
	Prefix       string
	Brokers      []string
	Topic        string
	URL          string
}

func (w *wizard) pickSink(ctx context.Context, region string) (*sinkChoice, error) {
	fmt.Fprintln(w.Out, "\nWhere should the logs go?")
	fmt.Fprintln(w.Out, "   1) S3          — cheap durable archive (the classic use)")
	fmt.Fprintln(w.Out, "   2) Kafka       — stream into your existing pipeline")
	fmt.Fprintln(w.Out, "   3) HTTP        — POST batches to Datadog / Axiom / any webhook")
	fmt.Fprintln(w.Out, "   4) stdout      — pipe to vector / fluent-bit / jq, zero setup")
	sel, err := w.ask("sink", "1")
	if err != nil {
		return nil, err
	}
	switch strings.ToLower(sel) {
	case "1", "s3":
		return w.pickS3(ctx, region)
	case "2", "kafka":
		return w.pickKafka()
	case "3", "http":
		hookURL, err := w.ask("webhook URL (see docs/integrations/ for vendor recipes)", "")
		if err != nil {
			return nil, err
		}
		if hookURL == "" {
			return nil, fmt.Errorf("a URL is required")
		}
		return &sinkChoice{Type: "http", URL: hookURL}, nil
	case "4", "stdout":
		return &sinkChoice{Type: "stdout"}, nil
	default:
		return nil, fmt.Errorf("unknown sink choice %q", sel)
	}
}

func (w *wizard) pickS3(ctx context.Context, region string) (*sinkChoice, error) {
	var bucket string
	if w.ListBuckets != nil {
		if buckets, err := w.ListBuckets(ctx, region); err == nil && len(buckets) > 0 {
			fmt.Fprintln(w.Out, "\nS3 buckets:")
			for i, b := range buckets {
				fmt.Fprintf(w.Out, "  %2d) %s\n", i+1, b)
			}
			sel, err := w.ask("bucket (number or name)", "")
			if err != nil {
				return nil, err
			}
			if n, err := strconv.Atoi(sel); err == nil && n >= 1 && n <= len(buckets) {
				bucket = buckets[n-1]
			} else {
				bucket = sel
			}
		}
	}
	if bucket == "" {
		var err error
		bucket, err = w.ask("S3 bucket name", "")
		if err != nil {
			return nil, err
		}
		if bucket == "" {
			return nil, fmt.Errorf("a bucket is required")
		}
	}
	prefix, err := w.ask("key prefix", "rds/")
	if err != nil {
		return nil, err
	}
	// Buckets are regional and need not live where the databases do.
	bucketRegion, err := w.ask("bucket region", region)
	if err != nil {
		return nil, err
	}
	return &sinkChoice{Type: "s3", Bucket: bucket, BucketRegion: bucketRegion, Prefix: prefix}, nil
}

func (w *wizard) pickKafka() (*sinkChoice, error) {
	brokers, err := w.ask("Kafka brokers (comma-separated host:port)", "")
	if err != nil {
		return nil, err
	}
	var list []string
	for _, b := range strings.Split(brokers, ",") {
		if b = strings.TrimSpace(b); b != "" {
			list = append(list, b)
		}
	}
	if len(list) == 0 {
		return nil, fmt.Errorf("at least one broker is required")
	}
	topic, err := w.ask("topic", "rds-logs")
	if err != nil {
		return nil, err
	}
	return &sinkChoice{Type: "kafka", Brokers: list, Topic: topic}, nil
}

// render produces the commented YAML. Every user-provided scalar is emitted
// double-quoted (YAML double-quoted style ⊇ Go's %q escaping for printable
// input), so values containing "#", ": ", or bare booleans can't silently
// reshape the document.
func render(region string, sources []source, sink *sinkChoice) string {
	var b strings.Builder
	b.WriteString("# Generated by `rdstail init`. Full reference: README.md#configuration\n")
	b.WriteString("sources:\n")
	for _, s := range sources {
		fmt.Fprintf(&b, "  - type: rds\n    engine: %s\n    region: %q\n    instances: [%s]\n",
			s.Engine, region, quoteList(s.Instances))
	}

	b.WriteString("\nsinks:\n")
	switch sink.Type {
	case "s3":
		fmt.Fprintf(&b, "  - name: s3-archive\n    type: s3\n    s3:\n      bucket: %q\n      prefix: %q\n      region: %q\n",
			sink.Bucket, sink.Prefix, sink.BucketRegion)
	case "kafka":
		fmt.Fprintf(&b, "  - name: kafka\n    type: kafka\n    kafka:\n      brokers: [%s]\n      topic: %q\n",
			quoteList(sink.Brokers), sink.Topic)
	case "http":
		fmt.Fprintf(&b, "  - name: webhook\n    type: http\n    http:\n      url: %q\n      gzip: true\n", sink.URL)
		b.WriteString("      # auth header? keep the secret in the environment:\n")
		b.WriteString("      # headers:\n      #   Authorization: Bearer ${API_TOKEN}\n")
	case "stdout":
		b.WriteString("  - name: pipe\n    type: stdout\n    stdout:\n      format: ndjson\n")
	}

	b.WriteString("\nstate:\n  type: sqlite\n  path: ./rdstail-state.db\n")
	b.WriteString("\nruntime:\n  poll_interval: 10s\n  start_from: end   # tail from now; \"beginning\" replays retained history\n")
	if sink.Type == "stdout" {
		b.WriteString("\nmetrics:\n  enabled: false   # keep the process pipe-friendly\n")
	} else {
		b.WriteString("\nmetrics:\n  enabled: true\n  listen: :9090\n")
	}
	return b.String()
}

// quoteList renders a YAML flow sequence of double-quoted scalars.
func quoteList(items []string) string {
	quoted := make([]string, len(items))
	for i, s := range items {
		quoted[i] = strconv.Quote(s)
	}
	return strings.Join(quoted, ", ")
}
