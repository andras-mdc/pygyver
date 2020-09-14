## Main features

### GCSExecutor

Python utility to perform task with the Google Cloud Storage API

#### Example

```python
from pygyver.etl.storage import GCSExecutor

# Login
gcs = GCSExecutor()

# Upload local file tmp/file.pkl into data/file.pkl in GCS
gcs.upload_file(
    file_path='/tmp/file.pkl',
    gcs_path='data/file.pkl'
)
```
