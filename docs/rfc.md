# FollowTheMoney Data Lake Spec

This specification defines a data lake structure for use with [OpenAleph](https://openaleph.org), [OpenSanctions](https://opensanctions.org) and related systems. The idea to to produce a long-term storage mechanism for investigative graph data, both in source, intermediate and processed form.

!!! warning
    `ftm-lakehouse` is currently in an early R&D phase. The path conventions described here may not be in line with the current implementation, and the specification is subject to change. [Read the discussion here](https://aleph.discourse.group/t/rfc-followthemoney-data-lake-specification/276)

## Core concepts

- `Datasets` are logical units of source data, often representing a data origin, such as an official register or publication, or a leak of documents.
- `Data catalogs` are index files that help make indiviudal datasets more easily discoverable.
- `Entity files` are data files in which individual [FollowTheMoney](https://followthemoney.tech) entities are stored in a ready-to-index form, ie. they've been aggregated from fragments or statements. An indexer may need to add authorization information and apply denormalisations as needed.
- `Archive objects` are files that represent source or intermediate document formats used in document forensics. They're referenced from FtM entities via their SHA1 content checksum.


## Function

The idea of a FtM data lake is to provide a platform for multi-stage processing of data into FtM format. Keeping this data at rest (rather than, for example, in an Aleph operational database, and in `followthemoney-store`) promises modularity, simpler architecture, and cost effectiveness.

The fundamental idea is to have a convention-based file system layout with well-known paths for metadata, and for information interchange between different processing stages.


## Basic layout

A data lake file system may need to be able to hold metadata headers (e.g. `Content-Disposition`, `Content-Type`), so its better to think of this as object storage (S3, GCS, MinIO) than a plain operating system FS.

```bash
lakehouse/
    index.json                      # catalog index
    config.yml                      # catalog configuration

    versions/                       # versioned snapshots
        YYYY/MM/YYYY-MM-DDTHH:MM:SS/
            index.json
            config.yml

    [dataset]/
        index.json                  # dataset index
        config.yml                  # dataset configuration
        statistics.json             # computed statistics

        versions/                   # versioned snapshots
            YYYY/MM/YYYY-MM-DDTHH:MM:SS/
                index.json
                config.yml

        .LOCK                       # dataset-wide lock
        .locks/lakehouse/           # specific operation locks
        .cache/lakehouse/           # dataset cache

        archive/
            # SHA1 checksum split into directory segments:
            00/de/ad/
                00deadbeef123456789012345678901234567890           # file blob
                00deadbeef123456789012345678901234567890.json      # file metadata
                00deadbeef123456789012345678901234567890.txt       # extracted text

        mappings/
            [content_hash]/
                mapping.yml         # mapping configuration

        entities/
            statements/             # Delta Lake statement store
                origin=[origin]/    # partitioned by origin
                    *.parquet       # statement data

        entities.ftm.json           # aggregated entities export

        exports/
            statistics.json         # entity counts, pre-computed facets
            statements.csv          # complete sorted statements
            graph.cypher            # neo4j export (optional)

        jobs/
            runs/
                [job_type]/
                    [timestamp].json  # job run results

        tags/                       # workflow state tracking
            statements/
                last_updated
            journal/
                last_updated
                last_flushed
                flushing            # lock for flush operation
            mappings/
                [content_hash]/
                    last_processed
                    config_updated
```

Some thoughts on this:

- The entity data is not versioned here. In OpenSanctions, we're actually using a subfolder called `artifacts/[run_id]` to identify different ETL runs. This may not apply as well to Aleph, since it has no strong segregation of individual ETL runs.
    - In the current implementation for the [deltalake](https://delta-io.github.io/delta-rs/) statement store data is versioned, but the versions not necessarily match to specific ETL runs.
- This still doesn't have a nice way to do garbage collection on the archive without refcounting on entities.
- We may want the entity object structure in the lake to be a new format, e.g. with a `dataset` field and `statements` lists on each entity (instead of `properties`).

## Meet the daemons

### Entity aggregator

A service that would traverse all individual statement files in the `entities/statements` folder, sort them into a combined order and then emit aggregated FtM entities.

Ideas: DuckDB doing a big fat UNION on the CSV files right from the bucket, or some monstrous Java MapReduce/Spark thing that is good at sorting a terabyte without breaking a sweat. (Output does not have to be FtM entities - a combined & sorted `statements.csv` has the same effect of making the data indexable).

See also:

- `ftm4 aggregate-statements` command reading sorted statements to emit entities.

### Entity analyzer

A service that would read `entities.ftm.json` and does analysis on (a filtered, subset of) the entities, e.g. NER, language detection, translations, vectorization. New statements are chunked and written back to the lake.

These micro services are already built with this lake concept in mind:

- [ftm-analyze](https://docs.investigraph.dev/lib/ftm-analyze/)
- [ftm-geocode](https://docs.investigraph.dev/lib/ftm-geocode/)
- [ftm-transcribe](https://github.com/openaleph/ftm-transcribe)
- [ftm-assets](https://github.com/dataresearchcenter/ftm-assets)

### Entity indexer

    logstash -j128 -i s3://lake/[dataset]/entities.ftm.json

### File ingestor

Reads uploaded documents from `entities/crud` (?) and then drops statement files into the statement folder every 60 MB (or after each document?).

If the backend supports notifications (eg. via SQS, PubSub), then the act of dropping a file to one `origin`/`phase` folder could trigger the subsequent layer of processing.

- cf. https://min.io/docs/minio/linux/administration/monitoring/bucket-notifications.html

### Catalog collector

Goes through each dataset folder, and brings a reduced version of the dataset metadata into a big overview `catalog.json`. This then pretty directly travels into the `collections` Aleph database.

- Example: https://data.opensanctions.org/datasets/latest/index.json

## Concept for user edits (3rd party apps)

An app, e.g. for Network Diagrams, would fetch the complete `entities.ftm.json`, load it in a temporary store (e.g. DuckDB) and do read/write operations on it. After an edit session, the resulting store is exported back to the lake.

## Implementation stages

- FtM 4.0 with dataset and catalog metadata specs
- Make sure lake FS change notifications can be used for stage coupling
- Build an `followthemoney-store` dumper
- Find migration path for servicelayer `archive`

## Long-term implications

- This creates a flat-file alternative to `followthemoney-store`, using an external sorting mechanism to aggregate entity fragments.
- The `entities/crud/` section implicitly unifies the `document` and `entity` tables currently used in Aleph.
    - Introduces versioning - nice in WYSIWYG scenarios.
- No need to have `collection` table, index and `catalog.json`.
- While we're at it, mappings become flat files (as is right and proper) and can be run by a daemon.
- Lake folders can be copied between Aleph instances, making repeat processing (eg. leaks) unnecessary.
- Re-indexing gains a huge performance boost (no sorting `followthemoney-store` tables, efficient bulk indexing).
- Option for file-based inverted index building that can allow hi-performance cross-referencing (xref)
- Improved infrastructure cost-efficiency as `followthemoney-store` postgresql (and therefore SSD storage) is obsolete

## References

The outlined concept above is the result of a decade of open source tooling around this problem. A lot of experimental and production work has already been done within the FollowTheMoney/Aleph open source ecosystem:

- [servicelayer](https://github.com/alephdata/servicelayer/) as the core document storage layer for Aleph/OpenAleph
- [nomenklatura](https://github.com/opensanctions/nomenklatura) by [OpenSanctions](https://opensanctions.org) for the statement based entities model
- [anystore](https://docs.investigraph.dev/lib/anystore/) by [DARC](https://darc.li) as a generic key/value storage that can act both as a blob (file) storage backend or caching backend
- [leakrfc](https://docs.investigraph.dev/lib/leakrfc/) by [DARC](https://darc.li) that was a first experiment for a standardized way of storing distributed file archives that Aleph, OpenAleph, memorious and other clients can read and write to.
