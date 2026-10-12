# DANDI Cache: `valid-nwb-file-to-chunk-stats`

A mapping from the content ID of every valid NWB file on the DANDI archive to a summary of that file's HDF5 data layout — how its datasets are chunked and compressed — for estimating what it costs to stream the file.

The set of valid NWB files is taken from the [`content-id-to-valid-nwb-file`](https://github.com/dandi-cache/content-id-to-valid-nwb-file) cache, restricted to the entries it marked `true`. Each such file is streamed directly from the public DANDI S3 bucket with [remfile](https://github.com/flatironinstitute/remfile) and read with [h5py](https://www.h5py.org/) — never downloaded — and every dataset in it is measured.

The headline number is `total_chunks`: reading the whole file takes at least one range request per stored chunk, so it is a lower bound on the round trips a streaming client pays. `median_chunk_count` and `max_chunks_in_dataset` say whether that total is spread evenly across the file or concentrated in a single badly chunked dataset.

Each line of the derivatives is a JSON object of the form:

```json
{"<content_id>": {
  "n_datasets": 412,
  "n_chunked": 99,
  "total_chunks": 15703,
  "max_chunks_in_dataset": 9421,
  "median_chunk_count": 12.0,
  "n_compressed": 96,
  "total_logical_bytes": 84213902144,
  "n_virtual_or_external": 0
}}
```

| Field | Meaning |
| --- | --- |
| `n_datasets` | Number of HDF5 datasets in the file. Groups and attributes are not datasets and are not counted. |
| `n_chunked` | How many of those datasets use the chunked layout. |
| `total_chunks` | Sum of each dataset's chunk count — a lower bound on the range requests needed to read the whole file. A contiguous or compact dataset counts as one. |
| `max_chunks_in_dataset` | The largest per-dataset chunk count. |
| `median_chunk_count` | Median chunk count over the *chunked* datasets only; `null` when the file has none. |
| `n_compressed` | How many datasets have a non-empty filter pipeline. |
| `total_logical_bytes` | Sum over datasets of `product(shape) * dtype.itemsize` — the uncompressed size of the data, not the size of the file on disk. |
| `n_virtual_or_external` | Datasets excluded from every statistic above because their bytes live elsewhere (a virtual dataset's sources, or an external storage sidecar). |

The chunked and compressed fractions are `n_chunked / n_datasets` and `n_compressed / n_datasets`; only the counts are stored, since either fraction is a division away.

A dataset's chunk count is derived purely from its current shape and chunk shape as `product(ceil(shape[i] / chunks[i]))`, so measuring a file never reads its chunk indexes over the network. Unlimited (`maxshape`) dimensions are measured at their present extent.

Zarr assets are recorded as `null`: they have no HDF5 chunk layout to measure. A file with zero datasets yields zeros throughout, with `median_chunk_count` as `null`.

Updated frequently.

Primarily for use by developers.



## One-time use

`dist` publishes each output as a [Zarr](https://zarr.dev/) store: the cache itself is `derivatives/valid_nwb_file_to_chunk_stats.zarr/`.
A store is one array of [JSON Lines](https://jsonlines.org/) text cut into chunk files, each holding whole lines and gzip-compressed.
It can be read with the standard library alone, or opened remotely with any Zarr library.

### Python API (recommended)

The chunk files are gzip-compressed [JSON Lines](https://jsonlines.org/): list them, then read them in order as you would any JSONL files.

```python
import gzip, json, urllib.request

chunks = "https://api.github.com/repos/dandi-cache/valid-nwb-file-to-chunk-stats/contents/derivatives/valid_nwb_file_to_chunk_stats.zarr/stream/c?ref=dist"
files = sorted((entry["download_url"] for entry in json.load(urllib.request.urlopen(chunks))), key=lambda url: int(url.rsplit("/", 1)[1]))
valid_nwb_file_to_chunk_stats = {key: value for url in files for line in gzip.decompress(urllib.request.urlopen(url).read()).decode().splitlines() if line for key, value in json.loads(line).items()}
```

Each line is a single-entry mapping of `{"<content_id>": <chunk_statistics>}`.

### Remote Zarr store

Anything that opens Zarr can open the store over HTTP, reading only the chunks it needs.
The group's `zarr.json` carries the array's metadata, so opening it takes one request:

```bash
pip install "zarr>=3" fsspec aiohttp
```

```python
import json

import zarr

url = "https://raw.githubusercontent.com/dandi-cache/valid-nwb-file-to-chunk-stats/refs/heads/dist/derivatives/valid_nwb_file_to_chunk_stats.zarr"
stream = zarr.open_group(url, mode="r")["stream"]
print(stream.shape, stream.chunks)  # Bytes in all, and bytes per chunk.

# The first chunk, fetched and decompressed on its own: whole lines, each `{"<key>": <value>}`.
first_chunk = stream[: stream.chunks[0]].tobytes().decode("utf-8")
for line in first_chunk.splitlines():
    if line.strip():
        print(json.loads(line))
        break
```

Reading `stream[:]` returns the whole text; to read it in pieces, step through it by `stream.chunks[0]`, as each chunk is fetched whole.

### Save to file

A raw URL cannot be listed, so to fetch every chunk at once clone the `dist` branch, which holds nothing else:

```bash
git clone --branch dist --depth 1 https://github.com/dandi-cache/valid-nwb-file-to-chunk-stats.git
```



## Repeated use

If you plan on using this cache regularly, clone the `derivatives` branch of this repository:

```bash
git clone --branch derivatives https://github.com/dandi-cache/valid-nwb-file-to-chunk-stats.git
```

Or, if you prefer [DataLad](https://www.datalad.org/):

```bash
datalad clone https://github.com/dandi-cache/valid-nwb-file-to-chunk-stats.git --branch derivatives
```

Then set up a CRON on your system to pull the latest version of the cache at your desired frequency.

For example, through `crontab -e`, add:

```bash
0 0 * * * git -C /path/to/valid-nwb-file-to-chunk-stats pull
```

This will minimize data overhead by only loading the most recent changes.

Each output is a store on the `derivatives` branch, where it is the whole log, uncompacted, and plain text:

```python
import json, pathlib

chunks = sorted(pathlib.Path("valid-nwb-file-to-chunk-stats/derivatives/valid_nwb_file_to_chunk_stats.zarr/stream/c").iterdir(), key=lambda path: int(path.name))
valid_nwb_file_to_chunk_stats = {key: value for chunk in chunks for line in chunk.read_text().splitlines() if line for key, value in json.loads(line).items()}
```


## Reading the `derivatives` branch

Each output is kept on the `derivatives` branch as a [Zarr](https://zarr.dev/) v3 store of JSON Lines: a one-dimensional `uint8` array whose bytes are the lines, padded with newlines to the end of its last chunk.
A key recorded more than once keeps its last line, which the dictionary below does by itself.

```python
import json

import zarr  # zarr>=3, with fsspec and aiohttp to read over HTTP

url = "https://raw.githubusercontent.com/dandi-cache/valid-nwb-file-to-chunk-stats/derivatives/derivatives/valid_nwb_file_to_chunk_stats.zarr"
stream = zarr.open_group(url, mode="r")["stream"]
lines = stream[:].tobytes().decode("utf-8").splitlines()
valid_nwb_file_to_chunk_stats = {key: value for line in lines if line for key, value in json.loads(line).items()}
```

Without Zarr, each chunk is a plain file of whole lines at `derivatives/valid_nwb_file_to_chunk_stats.zarr/stream/c/<n>`, read in order of `n`.
