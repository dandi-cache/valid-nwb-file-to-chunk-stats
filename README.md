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

If you only plan to use this cache infrequently or from disparate locations, you can directly download the latest version of the cache as a compressed [JSON Lines](https://jsonlines.org/) file from the `dist` branch:

### Python API (recommended)

```python
import gzip
import json

import requests

url = "https://raw.githubusercontent.com/dandi-cache/valid-nwb-file-to-chunk-stats/refs/heads/dist/derivatives/valid_nwb_file_to_chunk_stats.jsonl.gz"
response = requests.get(url)
lines = gzip.decompress(data=response.content).decode("utf-8").splitlines()
valid_nwb_file_to_chunk_stats = [json.loads(line) for line in lines]
```

Each line is a single-entry mapping of `{"<content_id>": <chunk_statistics>}`.

### Save to file

```bash
curl https://raw.githubusercontent.com/dandi-cache/valid-nwb-file-to-chunk-stats/refs/heads/dist/derivatives/valid_nwb_file_to_chunk_stats.jsonl.gz -o valid_nwb_file_to_chunk_stats.jsonl.gz
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
