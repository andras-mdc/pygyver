# PyGyver

> PyGyver is a user-friendly python package for data integration and manipulation.

> Named after MacGyver, title character in the TV series MacGyver, and Python, the main language used in the repository.

## Installation

### PyPi

PyGyver is available on [PyPi](https://pypi.org/project/pygyver/).

```python 
pip install pygyver
```

### Setup

Most APIs requires access token files to authentificate and perform tasks such as creating or deleting objects. Those files need to be generated prior to using `pygyver` and stored in the environment you are executing your code against. The package make use of environment variables, and some of the below might need be supplied in your environment:

```
# Access token path
GOOGLE_APPLICATION_CREDENTIALS=path_to_google_access_token.json
FACEBOOK_APPLICATION_CREDENTIALS=path_to_facebook_access_token.json

# Default values
BIGQUERY_PROJECT=your-gcs-project
GCS_PROJECT=your-gcs-project
GCS_BUCKET=your-gcs-bucket

# Optional
PROJECT_ROOT=path_to_where_your_code_lives
```

## Modules

PyGyver is structured around several modules available in the `etl` folder. Here is a summary table of those modules:

| Module name | Descrition | Documentation |
| ------------- |-------------|-------------|
| `dw` | Perform task against the Google Cloud BigQuery API | [dw.md](docs/dw.md) |
| `facebook` | Perform task against the Facebook Marketing API | [facebook.md](docs/facebook.md) |
| `gooddata` | Perform task against the GoodData API | - |
| `gs` | Perform task against the Google Sheet API | - |
| `lib` | Store utilities used by other modules | - |
| `pipeline` | Utility to build data pipelines via YAML definition | [pipeline.md](docs/pipeline.md) |
| `prep` | Data transformation - ML pipelines | - |
| `storage` | Perform task against the AWS S3 and Google Cloud Storage API | [storage.md](docs/storage.md) |
| `toolkit` | Sets of tools for data manipulation | - |

In order to load `BigQueryExecutor` from the `dw` module, you can run:

```
from pygyver.etl.dw import BigQueryExecutor
```

## Contributing

> To get started...

### Step 1

- 👯 Clone this repo to your local machine using `git@github.com:madedotcom/pygyver.git`

### Step 2

- **HACK AWAY!** 🔨🔨🔨

The team follows TDD to develop new features on `pygyver`.
Tests can be found in `pygyver/tests`.

### Step 3

- 🔃 Create a new pull request and request review from team members. Where applicable, a test should be added with the code change.

## FAQ

- **How to release a new version to PyPi?**
    1. Merge your changes to `master` branch
    2. Create a new release using `https://github.com/madedotcom/pygyver/releases`
