# Replica validation metrics

Enable periodic validation and the Prometheus endpoint:

```yaml
addr: ":9091"
validation:
  interval: 15m
```

Scrape `/metrics` on the configured address. Validation checks the order and
continuity of LTX transaction ranges at each configured compaction level.
It does not read file contents, check SQLite integrity, or test a restore.

Each metric has `db` (the database path) and `level` (the compaction level)
labels. Prometheus adds target labels such as `job` and `instance`.

| Metric | Type | Meaning |
| --- | --- | --- |
| `litestream_validation_checks_total` | Counter | Checks completed, with a `result` label: `success`, `invalid`, or `error`. |
| `litestream_validation_success` | Gauge | `1` if the last check passed; `0` if it failed or the first check has not finished. |
| `litestream_validation_last_success_timestamp_seconds` | Gauge | Unix time of the last passing check, or `0` until a check passes. |

`invalid` means the check completed and found one or more missing, overlapping,
or unordered transaction ranges. The counter increases once per check, even if
the check finds multiple problems. `error` means the check could not complete,
including storage errors, cancellation, and timeouts. A successful check sets
the success gauge back to `1`. Failed checks preserve the last success time.

These metrics cover both periodic checks and explicit calls to `Store.Validate`.
Series first appear when a database level is checked. A database without a
replica is skipped. If a storage error stops validation, later database levels
are not checked and their metrics stay unchanged. An empty file listing passes
the continuity check; a successful check does not prove that a backup exists.

Metrics reset when the process restarts. Validation failures do not pause
replication, compaction, or retention, and do not start automatic repair.

## Example alerts

These examples assume a 15-minute validation interval. Set the job selector to
match your scrape configuration and adjust the durations for your interval.

```yaml
groups:
  - name: litestream-validation
    rules:
      - alert: LitestreamValidationFailed
        expr: litestream_validation_success{job="litestream"} == 0
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Replica validation failed for {{ $labels.db }} at level {{ $labels.level }}"
      - alert: LitestreamValidationStale
        expr: time() - litestream_validation_last_success_timestamp_seconds{job="litestream"} > 1800
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "No recent passing validation for {{ $labels.db }} at level {{ $labels.level }}"
```

Also alert on scrape failures and missing expected validation series. The rules
above cannot detect a series that never appeared, such as when validation is
disabled or the first check never reaches that database level. Use your target
and database inventory to identify which series must exist.

Use the `invalid` and `error` counter results to distinguish backup continuity
problems from failures to run the check. Inspect the validation logs for details
and test a restore before relying on the affected backup.
