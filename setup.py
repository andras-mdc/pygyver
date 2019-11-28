import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="py-analytics",
    version="0.0.1",
    author="Made.com",
    author_email="analytics@made.com",
    description="Data engineering & Data science Framework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/madedotcom/py-analytics",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)