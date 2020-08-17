import io
import versioneer

import setuptools


def read(*filenames, **kwargs):
    encoding = kwargs.get("encoding", "utf-8")
    sep = kwargs.get("sep", "\n")
    buf = []
    for filename in filenames:
        with io.open(filename, encoding=encoding) as f:
            buf.append(f.read())
    return sep.join(buf)


long_description = read("README.md")

setuptools.setup(
    name="pygyver",
    version=versioneer.get_version(),
    author="Simone Fiorentini",
    author_email="analytics@made.com",
    description="Data engineering & Data science Pipeline Framework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/madedotcom/pygyver",
    test_suite="pygyver.tests",
    packages=setuptools.find_packages(),
    install_requires=[
        'wheel',
        'PyYAML==5.3.1',
        'nltk==3.4.5',
        'boto3==1.9.218',
        'pyarrow==0.16.0',
        'facebook-business==6.0.0',
        'google-api-python-client==1.10.0',
        'google-auth==1.20.1',
        'google-cloud-storage==1.30.0',
        'google-cloud-bigquery==1.26.1',
        'gspread==3.6.0',
        'gspread-dataframe==3.0.7',
        'pandas>=1.0.0',
        'numpy>=1.17.4',
        'pandas-gbq>=0.9.0'
    ],
    cmdclass=versioneer.get_cmdclass(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Natural Language :: English",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
