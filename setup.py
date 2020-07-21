from setuptools import setup

setup(
    name="s3seek",
    version="1.0.0",
    author="Adam Faulconbridge",
    author_email="afaulconbridge@googlemail.com",
    packages=["s3seek"],
    description="File-like classes for interacting with AWS S3 buckets. In particular, for seek and partial download when reading.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/sanogenetics/s3seek",
    install_requires=[],
    extras_require={
        "dev": [
            "pytest-cov",
            "flake8",
            "black",
            "pylint",
            "pip-tools",
            "pipdeptree",
            "pre-commit",
            "boto3",
        ],
    },
)
