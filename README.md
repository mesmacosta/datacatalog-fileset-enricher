# datacatalog-fileset-enricher

A Python package to enrich Google Cloud Data Catalog Fileset Entries with tags.

[![CircleCI][1]][2]

## 1. Environment setup

### 1.1. Get the code

````bash
git clone https://github.com/mesmacosta/datacatalog-fileset-enricher
cd datacatalog-fileset-enricher
````

### 1.2. Auth credentials

##### 1.2.1. Create a service account and grant it below roles

- Data Catalog Editor
- Cloud Storage Editor

##### 1.2.2. Download a JSON key and save it as
- `./credentials/datacatalog-fileset-enricher.json`

### 1.3. Virtualenv

Using *virtualenv* is optional, but strongly recommended unless you use [Docker](#14-docker).

##### 1.3.1. Install Python 3.6+

##### 1.3.2. Create and activate an isolated Python environment

```bash
pip install --upgrade virtualenv
python3 -m virtualenv --python python3 env
source ./env/bin/activate
```

##### 1.3.3. Install the dependencies

```bash
pip install --upgrade --editable .
```

##### 1.3.4. Set environment variables

```bash
export GOOGLE_APPLICATION_CREDENTIALS=./credentials/datacatalog-fileset-enricher.json
```

### 1.4. Docker

Docker may be used as an alternative to run all the scripts. In this case, please disregard the [Virtualenv](#13-virtualenv) install instructions.

## 2. Enrich DataCatalog Fileset Entry with Tags

### 2.1. python main.py

- python

```bash
python main.py --project-id=my_project \
  enrich-gcs-filesets
```

- docker

```bash
docker build --rm --tag datacatalog-fileset-enricher .
docker run --rm --tty -v your_credentials_folder:/data datacatalog-fileset-enricher \
  --project-id=my_project \
  enrich-gcs-filesets
```

### 2.1. python clean up template and tags (Reversible)
Cleans up the Template and Tags from the Fileset Entries, running the main command will recreate those.

```bash
python main.py --project-id=my_project \
  clean-up-templates-and-tags
```

### 2.2.  python clean up all (Non Reversible, be careful)
Cleans up the Fileset Entries, Template and Tags. You have to re create the Fileset entries if you need to restore the state,
which is outside the scope of this script.

```bash
python main.py --project-id=my_project \
  clean-up-all
```

[1]: https://circleci.com/gh/mesmacosta/datacatalog-fileset-enricher.svg?style=svg
[2]: https://circleci.com/gh/mesmacosta/datacatalog-fileset-enricher
