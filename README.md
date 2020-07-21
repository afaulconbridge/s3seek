s3seek
======

[![Build Status](https://circleci.com/gh/sanogenetics/s3seek.svg?style=svg)](https://app.circleci.com/pipelines/github/sanogenetics/s3seek)
[![PyPI version](https://badge.fury.io/py/s3seek.svg)](https://badge.fury.io/py/s3seek)

File-like classes for interacting with AWS S3 buckets. In particular, for
seek and partial download when reading.

Based on https://alexwlchan.net/2019/02/working-with-large-s3-objects/

development
-----------

```sh
pip install -e .[dev]  # Install using pip including development extras
pre-commit install  # Enable pre-commit hooks
pre-commit run --all-files  # Run pre-commit hooks without committing
# Note pre-commit is configured to use:
# - seed-isort-config to better categorise third party imports
# - isort to sort imports
# - black to format code
pip-compile  # Freeze dependencies
pytest  # Run tests
coverage run --source=s3seek -m pytest && coverage report -m  # Run tests, print coverage
mypy .  # Type checking
pipdeptree  # Print dependencies
```

Global git ignores per https://help.github.com/en/github/using-git/ignoring-files#configuring-ignored-files-for-all-repositories-on-your-computer

For release to PyPI see https://packaging.python.org/tutorials/packaging-projects/
