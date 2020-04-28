import setuptools


setuptools.setup(
    name="pygyver",
    version="0.0.1",
    author="Made.com",
    author_email="analytics@made.com",
    description="Data engineering & Data science Framework",
    long_description="Data engineering & Data science Framework",
    long_description_content_type="text/markdown",
    url="https://github.com/madedotcom/pygyver",
    packages=setuptools.find_packages(),
    install_requires=[
        'nltk==3.4.5',
        'PyYAML==5.1.2',
        'boto3==1.9.218',
        'pyarrow==0.16.0',
        'facebook-business==6.0.0',
        'gspread==3.1.0',
        'google-api-python-client==1.7.11',
        'oauth2client==4.1.3'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
