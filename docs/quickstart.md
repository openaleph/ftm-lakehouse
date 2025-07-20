# Quickstart

## Install

Requires python 3.11 or later.

```bash
pip install ftm-lakehouse
```

## Build a dataset

`ftm-lakehouse` stores _metadata_ for the files that then refers to the actual _source files_.

For example, take this public file listing archive: [https://data.ddosecrets.com/Patriot%20Front/patriotfront/2021/Organizational%20Documents%20and%20Notes/](https://data.ddosecrets.com/Patriot%20Front/patriotfront/2021/Organizational%20Documents%20and%20Notes/)

Crawl these documents into a _dataset_:

```bash
ftm-lakehouse -d ddos_patriotfront crawl "https://data.ddosecrets.com/Patriot%20Front/patriotfront/2021/Organizational%20Documents%20and%20Notes"
```

The _metadata_ and _source files_ are now stored in the archive (`./data` by default).

## Inspect files and archive

All _metadata_ and other information lives in the `ddos_patriotfront/.ftm-lakehouse` subdirectory. Files are keyed and accessible by their (relative) path.

Retrieve file metadata:

```bash
ftm-lakehouse -d ddos_patriotfront head Event.pdf
```

Retrieve actual file blob:

```bash
ftm-lakehouse -d ddos_patriotfront get Event.pdf > Event.pdf
```

Show all files metadata present in the dataset archive:

```bash
ftm-lakehouse -d ddos_patriotfront ls
```

Show only the file paths:

```bash
ftm-lakehouse -d ddos_patriotfront ls --keys
```

Show only the checksums (sha1 by default):

```bash
ftm-lakehouse -d ddos_patriotfront ls --checksums
```

### Tracking changes

The [`make`](./make.md) command (re-)generates the datasets metadata.

Delete a file:

```bash
rm ./data/ddos_patriotfront/Event.pdf
```

Now regenerate:

```bash
ftm-lakehouse -d ddos_patriotfront make
```

The result output will indicate that 1 file was deleted.

## configure storage

```yaml
storage_config:
  uri: s3://my_bucket
  backend_kwargs:
    endpoint_url: https://s3.example.org
    aws_access_key_id: ${AWS_ACCESS_KEY_ID}
    aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY}
```

### dataset config.yml

Follows the specification in [`ftmq.model.Dataset`](https://github.com/dataresearchcenter/ftmq/blob/main/ftmq/model/dataset.py):

```yaml
name: my_dataset #  also known as "foreign_id"
title: An awesome leak
description: >
  Incidunt eum asperiores impedit. Nobis est dolorem et quam autem quo. Name
  labore sequi maxime qui non voluptatum ducimus voluptas. Exercitationem enim
  similique asperiores quod et quae maiores. Et accusantium accusantium error
  et alias aut omnis eos. Omnis porro sit eum et.
updated_at: 2024-09-25
index_url: https://static.example.org/my_dataset/index.json
# add more metadata

ftm-lakehouse: # see above
```
