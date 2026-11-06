# Page execution handoff (stage 5 history)

For the current NETWORK/COPY split and shared originals, see [Shared original sources](SHARED-ORIGINALS.md).

The page paths already using Manager now obtain a persisted ContentQueueStore lease before starting storage preparation or a transfer (including aria2 dispatch). PageQueueOwner is owned by the process Manager; opening the store alone never resets running workers. The existing screen API and page executors remain in use.

Admission commits the old Room pending record first, then the independent queue. Only the latter grants execution. A crash between commits leaves a replayable legacy intent. Import uses `page:<uuid>` and validates its complete output/resource identity. Existing core state wins; completed/cancelled tombstones prevent stale legacy rows from resurrecting work. Explicit submissions can replace waiting intents, but cannot revoke live output ownership.

Pause, network parking, failure and cancellation keep the permit until worker cleanup exits. A resume during stopping records a deferred yield. A committed output retains capacity through cleanup; process-death recovery marks that acknowledged output successful. Completion and cancel acknowledgements update the compatibility journal without modifying completed-download tables, Room version, recorded-page probes or library queries. Failures to persist a worker acknowledgement retain the in-memory reservation and surface an error; process restart recovers it after the old worker is gone.

All page phases conservatively hold a NETWORK permit. Separate COPY/ENCODE scheduling, shared-resource transfer reuse, other download entrypoints, cache/output receipt reconciliation and device validation remain future work. The filesystem commit and database acknowledgement are separate operations: this phase does not promise exactly-once filesystem writes after a crash in that interval. Completed/cancelled core payloads are compacted after journal cleanup, while identity tombstones remain; retention/garbage collection needs a later migration protocol.

Tests cover legacy-only replay, failed core admission, idempotent import, core-only recovery, committed/cancelled stale journals, stop/resume tokens, resource exclusion and the Manager execution boundary. CI results must be checked for the exact submitted commit before treating this stage as verified. No package, signing or version change is included.

## Regression against the November 5 incident

The preserved incident analysis reports 35 observations of 6–9 DOWNLOADING items (ceiling 5), 20 download failures (15 timeout; 2 unexpected), and two process changes near original-image display. The original attachment is no longer present in this execution environment; these facts come from the preserved ANALYSIS.md. They do not establish the cause of process termination.

Manual and automatic Manager starts now share the capacity gate; cancellation and cleanup keep permits. Admission and claims are off main. Lifecycle events (submit/admitted/claim/wait/ack) include UUID/artwork/page/PID and no resource URL. Transport, preparation and remote-dispatch errors include stack traces through Timber, so export preserves more than an Analytics `unexpected` category. On Android 11+, the owner reads at most three historical process exits on IO, recording reason/status/time/PSS/RSS alongside current build metadata. A missing OS record does not exclude a crash or ANR; no trace stream is read on startup.

Storage finishWrite now precedes record publication and active-row removal. A thrown commit failure reaches the existing worker error handler, retains staging data and marks the page failed instead of recording durable core success. Existing completed-record formats and lookup queries remain unchanged.

Still required on device: saturate the queue with slow transfers, rapidly save/continue other large images, background/terminate/reopen, simulate storage failure, and revisit artwork 149861795 page 7 with adjacent pages. Confirm process-exit reasons and queue lifecycle together. This change does not claim to have fixed an unclassified image-decoder/native/system termination.
