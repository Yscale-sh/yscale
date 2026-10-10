# Preparing model weights on a Linode volume

Yscale's `spec.modelVolume` path consumes an existing Linode block volume. It
does not download or seed weights for you. The volume and burst must be in the
same region, and only one burst can attach the volume at a time.

Before creating resources, confirm the provider's current compute, storage and
transfer prices and your model's access/license requirements. Budget for rounded
billing periods and cleanup. Reuse one temporary helper for a seeding session.

1. Create a volume with a stable label and enough capacity for the model.
2. Attach it to an isolated helper in the burst region. Verify the exact device
   and that it is empty before formatting; formatting destroys existing data.
3. Put the Hugging Face cache under an `hf/` directory at the filesystem root.
   Download and verify the model using your authorized model credentials.
4. Flush writes, unmount and detach the volume. Keep the volume; delete the
   temporary helper and verify its absence with the provider.
5. Submit an explicitly Linode-routed workload with `spec.modelVolume` set to
   the volume label. Other backends and auto routing do not accept this field.

The bootstrap mounts the volume read-only at `/mnt/model-cache`. The workload
translator exposes it at `/models` and sets `HF_HOME=/models/hf` and
`HF_HUB_OFFLINE=1`. Missing or incorrectly laid-out weights cannot be repaired
by an offline download inside the job. Release the volume from any helper before
submitting the workload, and serialize workloads sharing it.

For object-store inputs, writable scratch space and artifact outputs, see
[storage](storage.md). The [storage benchmark](benchmarks/storage-2026-09-24.md)
measures transport paths, not model inference or universal provider performance.
