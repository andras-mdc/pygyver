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
        'boto3>=1.9.218',
        'codecov>=2.1.0',
        'facebook-business>=6.0.0',
        'google-cloud-bigquery>=1.24.0',
        'google-cloud-storage>=1.28.0',
        'gspread>=3.1.0',
        'gspread-dataframe>=3.0.4',
        'moto>=1.3.14',
        'nltk>=3.4.0',
        'numpy>=1.13.3',
        'pandas>=1.0.0',
        'pandas-gbq>=0.9.0',
        'pg8000>=1.14.0',
        'pyarrow>=1.0.0',
        'pymysql>=0.9.3',
        'pytest>=5.3.1',
        'PyYAML>=5.1',
        'sqlalchemy>=1.3.0',
        'tox>=3.15.0',
        'versioneer>=0.18',
        'wheel>=0.33.1'
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
