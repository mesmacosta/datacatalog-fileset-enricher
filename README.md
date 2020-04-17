# datacatalog-fileset-enricher

A Python package to enrich Google Cloud Data Catalog Fileset Entries with Data Catalog Tags. The goal of this library is to provide useful statistics regarding the GCS files that match the file pattern on the provided Data Catalog Fileset Entry.

For instructions on how to create Fileset Entries, please go to the official [Google Cloud Docs][5]

[![CircleCI][3]][4] [![CoverageStatus][1]][2]

## 1. Created Tags

Tags created by the fileset enricher are composed by the following attributes, and all stats are a snapshot of the
execution time:

| Field                      | Description                                                            | Mandatory |
| ---                        | ---                                                                    | ---       |
| **execution_time**         | Execution time when all stats were collected.                          | Y         |
| **files**                  | Number of files found, that matches the prefix.                        | N         |
| **min_file_size**          | Minimum file size found in bytes.                                      | N         |
| **max_file_size**          | Maximum file size found in bytes.                                      | N         |
| **avg_file_size**          | Average file size found in bytes.                                      | N         |
| **total_file_size**        | Total file size found in bytes.                                        | N         |
| **first_created_date**     | First time a file was created in the bucket(s).                        | N         |
| **last_created_date**      | Last time a file was created in the bucket(s).                         | N         |
| **last_updated_date**      | Last time a file was updated in the bucket(s).                         | N         |
| **created_files_by_day**   | Number of files created on the same date.                              | N         |
| **updated_files_by_day**   | Number of files updated on the same date.                              | N         |
| **prefix**                 | Prefix used to find the files.                                         | N         |
| **bucket_prefix**          | When specified at runtime, buckets without this prefix are ignored.    | N         |
| **buckets_found**          | Number of buckets that matched the prefix.                             | N         |
| **files_by_bucket**        | Number of files found on each bucket.                                  | N         |
| **files_by_type**          | Number of files found by file type.                                    | N         |

If no fields are specified when running the fileset enricher, all Tag fields will be applied.

To generate file statistics and create the Tags this python package, uses the GCS ````list_buckets```` and ````list_blobs```` APIs to extract the metadata that matches the file pattern, so their billing policies will apply.

## 2. Environment setup

### 2.1. Get the code

````bash
git clone https://github.com/mesmacosta/datacatalog-fileset-enricher
cd datacatalog-fileset-enricher
````

### 2.2. Auth credentials

##### 2.2.1. Create a service account and grant it below roles

- Data Catalog Tag Editor
- Data Catalog TagTemplate Owner
- Data Catalog Viewer
- Storage Admin or Custom Role with storage.buckets.list acl

##### 2.2.2. Download a JSON key and save it as
- `./credentials/datacatalog-fileset-enricher.json`

### 2.3. Virtualenv

Using *virtualenv* is optional, but strongly recommended unless you use [Docker](#24-docker).

##### 2.3.1. Install Python 3.6+

##### 2.3.2. Create and activate an isolated Python environment

```bash
pip install --upgrade virtualenv
python3 -m virtualenv --python python3 env
source ./env/bin/activate
```

##### 2.3.3. Install the dependencies

```bash
pip install --upgrade --editable .
```

##### 2.3.4. Set environment variables

```bash
export GOOGLE_APPLICATION_CREDENTIALS=./credentials/datacatalog-fileset-enricher.json
```

### 2.4. Docker

Docker may be used as an alternative to run all the scripts. In this case, please disregard the [Virtualenv](#23-virtualenv) install instructions.

## 3. Enrich DataCatalog Fileset Entry with Tags

### 3.1. python main.py - Enrich all fileset entries

- python

```bash
python main.py --project-id my_project \
  enrich-gcs-filesets
```

- docker

```bash
docker build --rm --tag datacatalog-fileset-enricher .
docker run --rm --tty -v your_credentials_folder:/data datacatalog-fileset-enricher \
  --project-id my_project \
  enrich-gcs-filesets
```

### 3.2. python main.py -- Enrich all fileset entries using template from a different Project

```bash
python main.py --project-id my_project \
  enrich-gcs-filesets \
  --tag-template-name projects/my_different_project/locations/us-central1/tagTemplates/fileset_enricher_findings
```

### 3.3. python main.py -- Enrich a single entry

```bash
python main.py --project-id my_project \
  enrich-gcs-filesets \
   --entry-group-id my_entry_group \
   --entry-id my_entry
```

### 3.4. python main.py -- Enrich a single entry, specifying desired tag fields
Users are able to choose the Tag fields from the list provided at [Tags](#1-created-tags)

```bash
python main.py --project-id my_project \
  enrich-gcs-filesets \
 --entry-group-id my_entry_group \
 --entry-id my_entry
 --tag-fields files,prefix
```

### 3.5. python main.py -- Pass a bucket prefix if you want to avoid scanning too many buckets
When the bucket_prefix is specified, the list_bucket api calls pass this prefix and avoid scanning buckets
that don't match the prefix. This only applies when there's a wildcard on the bucket_name, otherwise the
get bucket method is called and the bucket_prefix is ignored.

```bash
python main.py --project-id my_project \
  enrich-gcs-filesets \
 --bucket-prefix my_bucket
```

### 3.6. python create fileset enricher template in a project
Creates the fileset enricher template in a different project.

```bash
python main.py --project-id my_different_project \
    create-template
```


### 3.6. python clean up template and tags (Reversible)
Cleans up the Template and Tags from the Fileset Entries, running the main command will recreate those.

```bash
python main.py --project-id my_project \
  clean-up-templates-and-tags
```

### 3.7.  python clean up all (Non Reversible, be careful)
Cleans up the Fileset Entries, Template and Tags. You have to re create the Fileset entries if you need to restore the state,
which is outside the scope of this script.

```bash
python main.py --project-id my_project \
  clean-up-all
```

## Disclaimers

This is not an officially supported Google product.

[1]: https://coveralls.io/repos/github/mesmacosta/datacatalog-fileset-enricher/badge.svg?branch=master&kill_cache=3
[2]: https://coveralls.io/github/mesmacosta/datacatalog-fileset-enricher?branch=master
[3]: https://circleci.com/gh/mesmacosta/datacatalog-fileset-enricher.svg?style=svg
[4]: https://circleci.com/gh/mesmacosta/datacatalog-fileset-enricher
[5]: https://cloud.google.com/data-catalog/docs/how-to/filesets
