Crawl a local or remote location of documents (that supports file listing) into a `ftm-lakehouse` dataset. This operation stores the file metadata and actual file blobs in the [configured archive](./configuration.md).

This will create a new dataset or update an existing one. Incremental crawls are cached via the global [ftm-lakehouse cache](./cache.md).

Crawls can add files to a dataset but never deletes non-existing files.

## Basic usage

### Crawl a local directory

```bash
ftm-lakehouse -d my_dataset crawl /data/dump1/
```
### Crawl a http location

The location needs to support file listing.

In this example, archives (zip, tar.gz, ...) will be extracted during import.

```bash
ftm-lakehouse -d ddos_blueleaks crawl --extract https://data.ddosecrets.com/BlueLeaks/
```

### Crawl from a cloud bucket

In this example, only pdf files are crawled:

```bash
ftm-lakehouse -d my_dataset crawl --include "*.pdf" s3://my_bucket/files
```

Under the hood, `ftm-lakehouse` uses [anystore](https://docs.investigraph.dev/lib/anystore) which uses [fsspec](https://filesystem-spec.readthedocs.io/en/latest/index.html) that allows a wide range of filesystem-like sources. For some, installing additional dependencies might be required.

### Extract

Source files can be extracted during import using [patool](https://pypi.org/project/patool/). This has a few caveats:

- When enabling `--extract`, archives won't be stored but only their extracted members, keeping the original (archived) directory structure.
- This can lead to file conflicts, if several archives have the same directory structure (file.pdf from archive2.zip would replace the previous one):

```
archive1.zip
    subdir1/file.pdf

archive2.zip
    subdir1/file.pdf
```

- To avoid this, use `--extract-ensure-subdir` to create a sub-directory named by its source archive to place the extracted members into. The result would look like:

```
archive1.zip/subdir1/file.pdf
archive2.zip/subdir1/file.pdf
```

- If keeping the source archives is desired, use `--extract-keep-source`

## Include / Exclude glob patterns

Only crawl a subdirectory:

    --include "subdir/*"

Exclude .txt files from a subdirectory and all it's children:

    --exclude "subdir/**/*.txt"


## Reference

::: ftm_lakehouse.crawl
