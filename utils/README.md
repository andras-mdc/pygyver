
# Common Python Utilities for Classification & Regression on GCP 

Some common tasks are written in the utils folder of this repository.

These should:
 - Help move data and models between BigQuery and GCS ("gcp" module)
 - Help perform consistent pre-processing in model training and prediction ("prep" module)
 - Plot common accuracy measures for classification & regression ("plots" module)

### Requirements

1. Access Tokens: You will need to login to the GCP console and create an access token. 
You could save this in User/<name./.gcs/service_account.json to mirror the folder hierarchy likely for AWS. 
You should now set an environment variable, 'GOOGLE_APPLICATION_CREDENTIALS', equal to the filepath. 
This can be done by adding a line to your .bash_profile:

    ```
    export GOOGLE_APPLICATION_CREDENTIALS=path/to/key.json
    ```

    You shouldn't use a different name for the variable, 
    since Google BigQuery & Storage APIs default to look for 'GOOGLE_APPLICATION_CREDENTIALS'.
    
    PyCharm only recognises system variables that were declared before PyCharm is installed. 
    Remaining enviroment variables are managed on a project-by-project basis:  
    Preferences > Build, Execution, Deployment > Console > Python Console
    
    This repository will also use a default Storage bucket from the environment variable 'GCS_BUCKET'. 
    You might also want to set this. For example:
    
    ```
    export GCS_BUCKET=my-bucket-name
    ```

2. gcloud SDK: This needs to be installed on the local machine, and included in your path.

3. virtual-environment: ensure the packages in requirements.txt are installed in your environment. 
The following code will achieve this from a Python Console:
    
    ```python
    from utils.envs import *
    install_requirements()
    ```
    
    Or from the terminal:
    
    ```
    pip install -r <path/to/project/>requirements.txt
    ```

### Jupyter Notebooks

It's difficult to get Jupyter notebooks and virtual environments to play nice. This is a bodge, but it will work:
Open a terminal and make sure your virtual environment is not activated, then launch a notebook:

```
deactivate
jupyter notebook
```

Then at the start of your notebook, you can update your PATH to access your venv,
 with precedence over any conda environment (if you launched Jupyter from a venv this isn't possible).

```python
import sys

# declare where your repo and venv reside:
root_dir = 'path/to/my/repo'
pkg_extension = '/venv/lib/python3.7/site-packages'

# Update Path to identify the environment packages, and repo modules respectively:
sys.path.insert(0, root_dir + pkg_extension)
sys.path.insert(0, root_dir)
```