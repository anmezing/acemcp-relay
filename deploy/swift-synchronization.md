# Swift Synchronization Accounting and Recovery

`codebase_swift_sync` uses its own tenant limit of 600 requests per minute.
It does not spend the daily interactive request allowance. Other tools,
including ordinary source indexing, retain their existing request accounting.
The console reports Swift synchronization separately using the normalized
request-log path `/mcp/tools/call/codebase_swift_sync`.

Uploads reserve decoded bytes from the existing index-byte pool before calling
LCE. Organization owner pools remain shared with source indexing. Canonical
base64, 256 KiB parts and at most 512 parts per job are enforced. A reservation
is keyed by tenant, root, job and part, with an immutable content digest. Redis
retains the part ledger for 48 hours, including across midnight. Identical
retries cost zero even on unlimited plans; quota rejections create no receipt.
Redis outages reject admission rather than bypassing accounting.

A reservation measures submitted bytes, not successful publications. A valid
part that LCE rejects still reserves its submitted bytes. Different content
cannot replace that part identity. The client resumes the same unexpired upload
after transport errors, including lost begin/upload responses. It reconciles
accepted publication before retrying and only replaces a job after confirming
that the prior job is terminal or cancelled. Uploads expire after 30 minutes;
accepted pending jobs do not expire merely because workers are busy.

The client caches one artifact per root with digest and source-identity checks,
128 MiB artifact and seven-day retention limits. Compilation has one global slot
and at most 32 queued requests. Queued cancellation returns immediately. Ready
roots use a lightweight status probe every minute; build context is rechecked
every five minutes and syntax plans batch at most eight paths / 4 MiB response.
All indexed files remain part of source identity because build inputs cannot be
inferred from file extensions. `LCE_SWIFT_SYNC=off` disables client compiler sync.

Graph preparation uses private transaction tables before taking the root lock.
It caps the root at 20,000 files, 500,000 facts and 256 MiB of serialized source
graph rows. Final publication rechecks the root revision, epoch, active source
jobs and worker lease. The final atomic graph replacement still scales with the
bounded root size; it does not provide constant-time root locking. The entire
import has a 15-minute deadline and a five-second lock-wait limit.
