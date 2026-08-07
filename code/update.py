import argparse
import itertools
import json
import math
import pathlib
import urllib.error
import urllib.request

import h5py
import numpy
import remfile

# Testing mode processes only this many items and writes to its own designated file
# (`derivatives/testing.jsonl`), leaving the real cache untouched.
_TESTING_LIMIT = 10
_CACHE_FILE_NAME = "valid_nwb_file_to_chunk_stats.jsonl"
_TESTING_FILE_NAME = "testing.jsonl"

# The input is the `content-id-to-valid-nwb-file` cache, registered as an input subdataset.
_INPUT_FILE_PATH = (
    pathlib.Path("sourcedata") / "content-id-to-valid-nwb-file" / "derivatives" / "content_id_to_valid_nwb_file.jsonl"
)

# The public DANDI archive S3 bucket. Every asset is content-addressed, so each valid NWB
# file is reachable directly from its content ID without consulting the DANDI API:
#   - HDF5 assets are stored as a single blob at `blobs/<c[:3]>/<c[3:6]>/<content_id>`.
#   - Zarr assets are stored as a directory store under `zarr/<content_id>/`.
# The content ID alone does not say which layout an entry uses, so the blob key is probed
# with a HEAD request first; a 404 means the entry is a Zarr store, which has no HDF5
# chunking to measure and is therefore recorded as `null` (see `_run`).
_BLOB_URL_TEMPLATE = "https://dandiarchive.s3.amazonaws.com/blobs/{prefix}/{infix}/{content_id}"

# Seconds to wait on the blob HEAD probe before treating it as a (transient) failure.
_PROBE_TIMEOUT = 30


def _load_content_id_to_validity(file_path: pathlib.Path) -> dict:
    """Load the `{content_id: bool}` mapping from the input JSONL, or an empty dict if missing."""
    records: dict = {}
    if not file_path.exists():
        return records
    with file_path.open(mode="r") as file_stream:
        for line in file_stream:
            if line.strip():
                records.update(json.loads(line))
    return records


def _load_previous_cache(file_path: pathlib.Path) -> dict:
    """Load the previously computed `{content_id: chunk_statistics}` mapping (empty on bootstrap)."""
    records: dict = {}
    if not file_path.exists():
        return records
    with file_path.open(mode="r") as file_stream:
        for line in file_stream:
            if line.strip():
                records.update(json.loads(line))
    return records


def _write_cache(file_path: pathlib.Path, records: dict) -> None:
    """Write the `{content_id: chunk_statistics}` mapping, one sorted content ID per line."""
    with file_path.open(mode="w") as file_stream:
        file_stream.writelines(f"{json.dumps({content_id: records[content_id]})}\n" for content_id in sorted(records))


def _dataset_layout_statistics(dataset: h5py.Dataset) -> dict | None:
    """
    Measure the data layout of a single HDF5 dataset.

    Returns `None` for a virtual or externally stored dataset: its bytes do not live in this
    file's own layout, so counting its chunks would not describe the cost of streaming *this*
    file. Everything else returns:

        {"logical_bytes": int, "layout": str, "chunks": tuple | None, "n_chunks": int,
         "is_compressed": bool}

    Every size is a Python int: the byte counts of a large NWB file overflow float precision.
    """
    creation_property_list = dataset.id.get_create_plist()
    layout_code = creation_property_list.get_layout()

    # A virtual dataset maps its data out of other files, and an external dataset stores its
    # bytes in a sidecar file; neither is part of this file's own chunk layout.
    if layout_code == h5py.h5d.VIRTUAL or creation_property_list.get_external_count() > 0:
        return None

    layout = {
        h5py.h5d.COMPACT: "compact",
        h5py.h5d.CONTIGUOUS: "contiguous",
        h5py.h5d.CHUNKED: "chunked",
    }.get(layout_code, "unknown")

    # An HDF5 "empty" dataset (null dataspace) has no shape at all and holds no elements;
    # `shape ()` is a scalar, which holds exactly one. `math.prod(())` is 1, so the scalar
    # case needs no special handling, but the null dataspace does.
    shape = dataset.shape
    n_elements = 0 if shape is None else math.prod(shape)
    logical_bytes = n_elements * dataset.dtype.itemsize

    # `dataset.chunks` is `None` for anything not chunked. The chunk grid is derived purely
    # from the current shape (which already resolves any unlimited `maxshape` dimension to
    # its present extent): `get_num_chunks()`/`get_storage_size()` would answer exactly this
    # but only by reading the chunk index over the network, which is what this cache exists
    # to help avoid paying for.
    chunks = dataset.chunks
    if chunks is None:
        n_chunks = 1
    else:
        n_chunks = math.prod(-(-shape[axis] // chunks[axis]) for axis in range(len(chunks)))

    return {
        "logical_bytes": logical_bytes,
        "layout": layout,
        "chunks": None if chunks is None else list(chunks),
        "n_chunks": n_chunks,
        "is_compressed": creation_property_list.get_nfilters() > 0,
    }


def compute_chunk_statistics(h5py_file: h5py.File) -> dict:
    """
    Summarize the data layout of every dataset in an open HDF5 file.

    `total_chunks` is the headline number: reading the whole file requires at least one range
    request per stored chunk, so it is a lower bound on the number of round trips a streaming
    client pays, and `median_chunk_count` / `max_chunks_in_dataset` say whether that total is
    spread evenly or concentrated in one badly chunked dataset.

    Groups and attributes are not datasets and are not counted. Virtual and external datasets
    are excluded from every statistic and reported on their own as
    `n_virtual_or_external`, since their bytes are not part of this file's layout.
    """
    per_dataset_statistics: list[dict] = []
    n_virtual_or_external = 0

    # A dataset reachable under several names must still be measured once. An object's address
    # (its file number plus its address within that file) identifies the underlying object
    # rather than the path used to reach it, so recording the ones already seen both
    # deduplicates multiply linked datasets and keeps a link cycle from looping.
    visited_object_ids: set[tuple[int, int]] = set()

    def _visit(_name: str, obj: object) -> None:
        nonlocal n_virtual_or_external
        if not isinstance(obj, h5py.Dataset):
            return
        object_info = h5py.h5o.get_info(obj.id)
        object_id = (object_info.fileno, object_info.addr)
        if object_id in visited_object_ids:
            return
        visited_object_ids.add(object_id)

        statistics = _dataset_layout_statistics(dataset=obj)
        if statistics is None:
            n_virtual_or_external += 1
            return
        per_dataset_statistics.append(statistics)

    h5py_file.visititems(_visit)

    n_datasets = len(per_dataset_statistics)
    chunk_counts = [statistics["n_chunks"] for statistics in per_dataset_statistics]
    chunked_chunk_counts = [
        statistics["n_chunks"] for statistics in per_dataset_statistics if statistics["chunks"] is not None
    ]
    n_chunked = len(chunked_chunk_counts)
    n_compressed = sum(1 for statistics in per_dataset_statistics if statistics["is_compressed"])

    return {
        "n_datasets": n_datasets,
        "n_chunked": n_chunked,
        "fraction_chunked": n_chunked / n_datasets if n_datasets > 0 else 0.0,
        "total_chunks": sum(chunk_counts),
        "max_chunks_in_dataset": max(chunk_counts, default=0),
        # The median is only meaningful over the datasets that actually have a chunk grid;
        # with none of them, there is no value to report.
        "median_chunk_count": float(numpy.median(chunked_chunk_counts)) if n_chunked > 0 else None,
        "n_compressed": n_compressed,
        "fraction_compressed": n_compressed / n_datasets if n_datasets > 0 else 0.0,
        # Summed as Python ints; the total across a large NWB file exceeds float precision.
        "total_logical_bytes": sum(statistics["logical_bytes"] for statistics in per_dataset_statistics),
        "n_virtual_or_external": n_virtual_or_external,
    }


def _blob_exists(url: str) -> bool:
    """Whether the content-addressed blob exists, i.e. the asset is HDF5 rather than Zarr."""
    request = urllib.request.Request(url=url, method="HEAD")
    try:
        with urllib.request.urlopen(url=request, timeout=_PROBE_TIMEOUT):
            return True
    except urllib.error.HTTPError as exception:
        if exception.code in (403, 404):
            # S3 answers a missing key with 404, or 403 when listing is denied to anonymous
            # callers; either way there is no blob here and the asset is a Zarr store.
            return False
        raise


def _compute_chunk_statistics_from_s3(content_id: str) -> dict:
    """Stream the HDF5 asset identified by `content_id` and summarize its data layout."""
    blob_url = _BLOB_URL_TEMPLATE.format(prefix=content_id[:3], infix=content_id[3:6], content_id=content_id)
    rem_file = remfile.File(url=blob_url)
    with h5py.File(name=rem_file, mode="r") as h5py_file:
        return compute_chunk_statistics(h5py_file=h5py_file)


def _run(base_directory: pathlib.Path, testing: bool, limit: int | None) -> None:
    content_id_to_validity = _load_content_id_to_validity(file_path=base_directory / _INPUT_FILE_PATH)
    # Only the assets the upstream cache marked valid ('true') are processed.
    valid_content_ids = {content_id for content_id, is_valid in content_id_to_validity.items() if is_valid is True}

    derivatives_directory = base_directory / "derivatives"
    derivatives_directory.mkdir(parents=True, exist_ok=True)
    cache_file_path = derivatives_directory / (_TESTING_FILE_NAME if testing else _CACHE_FILE_NAME)
    valid_nwb_file_to_chunk_stats = _load_previous_cache(file_path=cache_file_path)

    # Already-computed content IDs are exactly the keys already in the output, so re-runs skip
    # them and only pick up content IDs newly marked valid upstream.
    content_ids_to_process = sorted(valid_content_ids - valid_nwb_file_to_chunk_stats.keys())

    # A testing run caps the batch tightly; otherwise the optional `--limit` bounds a single
    # run because streaming and walking each file is heavy.
    effective_limit = _TESTING_LIMIT if testing else limit
    content_ids_to_process = list(itertools.islice(content_ids_to_process, effective_limit))

    for content_id in content_ids_to_process:
        blob_url = _BLOB_URL_TEMPLATE.format(prefix=content_id[:3], infix=content_id[3:6], content_id=content_id)
        try:
            if not _blob_exists(url=blob_url):
                # A Zarr store, which has no HDF5 chunk layout to measure. Recorded as `null`
                # rather than left out, so later runs skip it instead of re-probing it forever.
                print(f"Recording `{content_id}` as null: not an HDF5 blob (Zarr asset).", flush=True)
                valid_nwb_file_to_chunk_stats[content_id] = None
                continue
            chunk_statistics = _compute_chunk_statistics_from_s3(content_id=content_id)
        except Exception as exception:
            # These files were already opened successfully upstream, so a failure here is
            # almost always transient (network). Skip it and leave it for a later run to retry
            # rather than recording wrong statistics.
            print(f"Skipping `{content_id}`: {type(exception).__name__}: {exception}", flush=True)
            continue
        valid_nwb_file_to_chunk_stats[content_id] = chunk_statistics

    _write_cache(file_path=cache_file_path, records=valid_nwb_file_to_chunk_stats)


if __name__ == "__main__":
    default_base_directory = pathlib.Path(__file__).parent.parent

    parser = argparse.ArgumentParser(description="Update the valid-nwb-file-to-chunk-stats DANDI cache.")
    parser.add_argument(
        "--base-directory",
        type=pathlib.Path,
        default=default_base_directory,
        help=(
            "The directory containing the `sourcedata` and `derivatives` directories. "
            "Set to the mounted dataset path when run inside the pipeline container; "
            "defaults to the repository root."
        ),
    )
    parser.add_argument(
        "--testing",
        action="store_true",
        help=(
            f"Run in testing mode: process only the first {_TESTING_LIMIT} items and write "
            f"`derivatives/{_TESTING_FILE_NAME}` instead of the real cache, leaving it "
            "untouched. Omit for a complete update."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional cap on the number of newly valid content IDs to process in this run.",
    )
    args = parser.parse_args()

    _run(base_directory=args.base_directory, testing=args.testing, limit=args.limit)
