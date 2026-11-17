# TODO

- [ ] **Deferred: upstream Spark IPC producer API for native consumers.**

  Add a caller-provided output API, such as
  `writeNext(out: WritableByteChannel): Long`, to Spark's Arrow converter.
  Reuse the existing schema, record-batch and EOS serialization and cleanup.
  Keep `next(): Array[Byte]` as a compatibility wrapper. Native consumers can
  supply an output channel backed by native memory.

  Separately investigate driver-side task-result transport/deserialization that
  exposes retained native IPC buffers instead of reconstructing heap byte arrays.
  The converter API alone does not eliminate the remaining JNI copy for
  distributed queries; wrapping a received byte array in a direct buffer still
  requires a copy.

  Validate IPC compatibility, buffer lifetime, cancellation and cleanup with
  separate executor processes. Measure copy counts, memory and end-to-end client
  latency before claiming an improvement.

  Context: [native Spark Connect server PR #474](https://github.com/apache/spark-connect-rust/pull/474).
  Status: deferred for later; keep the current simplified IPC implementation.
