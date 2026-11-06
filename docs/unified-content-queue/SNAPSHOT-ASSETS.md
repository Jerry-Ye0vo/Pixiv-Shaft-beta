# Snapshot asset handoff

SnapshotGenerator now treats offline image localization as explicit queued work. Missing original pages, preview pages, author/comment avatars and stamps acquire a reference from the shared exact-URL source broker. This uses NETWORK authorization even for a small image because the consumer is an offline snapshot, while ordinary thumbnail display remains immediate.

Original requests always use the requested original URL and the broker's cache pin; a lower-resolution ready file cannot satisfy them. Preview requests can reuse a ready Glide file. If it disappears before COPY opens it, that local attempt fails and the same requested URL is acquired through the source queue. No original completed-library or duplicate record is created for a snapshot resource.

The source reference spans COPY waiting, actual byte transfer and close. A source dependency is resolved before COPY is claimed; no NETWORK slot is retained while waiting for a local copy. Existing per-snapshot parallelism only bounds planning width, while the shared SQLite claims authorize actual work across snapshots, original saves and translated images.

Every image/video copy and JSON/manifest write now enters a COPY phase. Bytes go to a unique sibling temporary file, are closed and fsynced, then renamed to the stable target. Input length is checked and cancellation before commit removes the temporary file. Failure cannot expose a truncated manifest or overwrite an existing target with an incomplete asset. SnapshotIndex is still published only after the manifest has committed. The existing asset mappings, original-quality flag, page order, comments, automatic/formal manifests, quota and ugoira directory format remain.

This phase covers SnapshotGenerator's byte paths, including manual generation, automatic snapshots and backfill that call this generator. It does not convert every snapshot management/archive/promote operation or introduce a replayable manual-snapshot parent. Backfill retains its existing persisted action parent. Full parent recovery, per-snapshot receipts, cache deletion coordination, task-center controls and device testing remain migration work.

Tests exercise source retention while COPY is saturated, requested quality, preview cache reuse/eviction fallback, cancelled waiting work, failed atomic commit retaining the existing target, and queued manifest writes. Existing path-safety, ugoira directory validation and ordered-parallelism suites remain selected alongside source, page, copy and ugoira regression.
