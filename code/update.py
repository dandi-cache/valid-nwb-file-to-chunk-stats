"""The HDF5 data layout of every valid NWB file, summarized per file.

`total_chunks` is the headline number: reading the whole file takes at least one range request per
stored chunk, so it is a lower bound on the round trips a streaming client pays, and
`median_chunk_count` / `max_chunks_in_dataset` say whether that total is spread evenly or
concentrated in one badly chunked dataset.

This is the one measure in the `valid-nwb-file-to-*` family that the shared structural walk cannot
supply. The walk describes the shape of the hierarchy; this describes each dataset's storage
layout, which is per-dataset HDF5 detail with exactly one consumer, so it stays here rather than
being pushed into a shared traversal that no other cache would use.

A Zarr asset has no HDF5 chunk layout to measure and is recorded as `null` rather than left out, so
later runs skip it instead of re-probing it forever.

Everything else -- the argument parsing, the logging, the batch cap, the error logs, the output
paths, testing mode, the layout probe and the streaming reader -- comes from `dandi_cache_utils`,
which the runtime image carries.
"""

import math

import dandi_cache_utils as dandi_cache
import numpy


def dataset_layout_statistics(h5py_dataset, /) -> dict | None:
    """Measure the data layout of a single HDF5 dataset.

    Returns `None` for a virtual or externally stored dataset: its bytes do not live in this file's
    own layout, so counting its chunks would not describe the cost of streaming *this* file.

    Every size is a Python int. The byte counts of a large NWB file overflow float precision.
    """
    import h5py

    creation_property_list = h5py_dataset.id.get_create_plist()
    layout_code = creation_property_list.get_layout()

    # A virtual dataset maps its data out of other files, and an external dataset stores its bytes
    # in a sidecar file; neither is part of this file's own chunk layout.
    if layout_code == h5py.h5d.VIRTUAL or creation_property_list.get_external_count() > 0:
        return None

    layout = {
        h5py.h5d.COMPACT: "compact",
        h5py.h5d.CONTIGUOUS: "contiguous",
        h5py.h5d.CHUNKED: "chunked",
    }.get(layout_code, "unknown")

    # An HDF5 "empty" dataset (null dataspace) has no shape at all and holds no elements; `shape
    # ()` is a scalar, which holds exactly one. `math.prod(())` is 1, so the scalar case needs no
    # special handling, but the null dataspace does.
    shape = h5py_dataset.shape
    number_of_elements = 0 if shape is None else math.prod(shape)
    logical_bytes = number_of_elements * h5py_dataset.dtype.itemsize

    # `chunks` is `None` for anything not chunked. The chunk grid is derived purely from the
    # current shape, which already resolves any unlimited `maxshape` dimension to its present
    # extent. `get_num_chunks()` / `get_storage_size()` would answer exactly this, but only by
    # reading the chunk index over the network, which is what this cache exists to help avoid.
    chunks = h5py_dataset.chunks
    if chunks is None:
        number_of_chunks = 1
    else:
        number_of_chunks = math.prod(-(-shape[axis] // chunks[axis]) for axis in range(len(chunks)))

    return {
        "logical_bytes": logical_bytes,
        "layout": layout,
        "chunks": None if chunks is None else list(chunks),
        "n_chunks": number_of_chunks,
        "is_compressed": creation_property_list.get_nfilters() > 0,
    }


def chunk_statistics(h5py_file, /) -> dict:
    """Summarize the data layout of every dataset in an open HDF5 file.

    Groups and attributes are not datasets and are not counted. Virtual and external datasets are
    excluded from every statistic and reported on their own as `n_virtual_or_external`, since their
    bytes are not part of this file's layout.
    """
    import h5py

    per_dataset_statistics: list[dict] = []
    number_virtual_or_external = 0

    # A dataset reachable under several names must still be measured once. An object's address (its
    # file number plus its address within that file) identifies the underlying object rather than
    # the path used to reach it, so recording the ones already seen both deduplicates multiply
    # linked datasets and keeps a link cycle from looping.
    visited_object_ids: set[tuple[int, int]] = set()

    def _visit(_name: str, obj: object) -> None:
        nonlocal number_virtual_or_external
        if not isinstance(obj, h5py.Dataset):
            return
        object_info = h5py.h5o.get_info(obj.id)
        object_id = (object_info.fileno, object_info.addr)
        if object_id in visited_object_ids:
            return
        visited_object_ids.add(object_id)

        statistics = dataset_layout_statistics(obj)
        if statistics is None:
            number_virtual_or_external += 1
            return
        per_dataset_statistics.append(statistics)

    h5py_file.visititems(_visit)

    chunk_counts = [statistics["n_chunks"] for statistics in per_dataset_statistics]
    chunked_chunk_counts = [
        statistics["n_chunks"] for statistics in per_dataset_statistics if statistics["chunks"] is not None
    ]
    number_chunked = len(chunked_chunk_counts)

    # The chunked and compressed fractions are `n_chunked / n_datasets` and `n_compressed /
    # n_datasets`; consumers can divide, so only the counts are stored.
    return {
        "n_datasets": len(per_dataset_statistics),
        "n_chunked": number_chunked,
        "total_chunks": sum(chunk_counts),
        "max_chunks_in_dataset": max(chunk_counts, default=0),
        # The median is only meaningful over the datasets that actually have a chunk grid; with
        # none of them, there is no value to report.
        "median_chunk_count": float(numpy.median(chunked_chunk_counts)) if number_chunked > 0 else None,
        "n_compressed": sum(1 for statistics in per_dataset_statistics if statistics["is_compressed"]),
        # Summed as Python ints; the total across a large NWB file exceeds float precision.
        "total_logical_bytes": sum(statistics["logical_bytes"] for statistics in per_dataset_statistics),
        "n_virtual_or_external": number_virtual_or_external,
    }


def compute_chunk_statistics(content_id, item) -> dict | None:
    """Summarize one asset's HDF5 layout, or record `None` where there is no HDF5 layout to read."""
    item.stage = "probing the asset layout"
    if dandi_cache.nwb.detect_layout(content_id) == dandi_cache.nwb.ZARR:
        return None

    item.stage = "reading the NWB file"
    h5py_file, _remote_file = dandi_cache.nwb.open_hdf5(dandi_cache.s3.blob_url(content_id))
    with h5py_file:
        return chunk_statistics(h5py_file)


def main() -> None:
    dataset, arguments = dandi_cache.open_dataset()

    # Only the assets the upstream cache marked valid are measured.
    validity = dataset.read_input()
    valid_content_ids = [content_id for content_id, is_valid in validity.items() if is_valid is True]

    dandi_cache.run_incremental_update(
        dataset,
        candidates=valid_content_ids,
        process=compute_chunk_statistics,
        limit=dandi_cache.effective_limit(testing=dataset.testing, limit=arguments.limit),
        # These files were already opened successfully upstream, so a failure here is almost always
        # transient. Leave the item for a later run rather than recording wrong statistics.
        on_failure=dandi_cache.SKIP,
        stages={
            "probing the asset layout": "layout_probe_errors.txt",
            "reading the NWB file": "file_read_errors.txt",
        },
        describe=lambda statistics: (
            "Zarr, no HDF5 layout" if statistics is None else f"{statistics['total_chunks']} chunks"
        ),
        checkpoint_every=50,
    )


if __name__ == "__main__":
    main()
