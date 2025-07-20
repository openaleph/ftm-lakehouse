A `ftm-lakehouse` archive can be configured via _environment variables_ or a yaml configuration file. Individual datasets within the archive can have their own configuration, which actually enables creating an archive with different _storage configurations_ per dataset.

## Using environment vars

Simply point to a local base folder containing the archive:

    LEAKRFC_URI=./data/

Or point to a (local or remote) yaml configuration (see below):

    LEAKRFC_URI=https://data.example.org/archive.yml

More granular config with more env vars. `ftm-lakehouse` uses [pydantic-settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/) to parse the configuration. Nested configuration keys can be accessed via `__` delimiter.

    LEAKRFC_ARCHIVE__URI=s3://ftm-lakehouse
    LEAKRFC_ARCHIVE__PUBLIC_URL=https://cdn.example.org/{dataset}/{key}
    LEAKRFC_ARCHIVE__STORAGE__READONLY=true

## YAML config

Create a base config and enable it via `LEAKRFC_URI=ftm-lakehouse.yml`:

```yaml
name: ftm-lakehouse-archive
storage:
  uri: ./archive
# ...
```

Within the local archive, one dataset could be actually living in the cloud:

`./archive/remote_dataset/.ftm-lakehouse/config.yml`:

```yaml
name: remote_dataset
storage:
  uri: s3://my_bucket/data
# ...
```

This means, the local folder `./archive/remote_dataset/` would only contain this yaml configuration and use the remote contents of the dataset.
