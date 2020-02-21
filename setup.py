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
        'nltk==3.4.4',
        'PyYAML==5.1.2'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
