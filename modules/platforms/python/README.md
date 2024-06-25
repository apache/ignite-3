# pyignite3
Apache Ignite 3 DB API Driver.

## Prerequisites

- Python 3.7 or above (3.7, 3.8, 3.9 and 3.10 are tested),
- Access to Ignite 3 node, local or remote.

## Installation

### From sources
This way is more suitable for developers or if you install client from zip archive.
1. Download and/or unzip Ignite 3 DB API Driver sources to `pyignite3_path`
2. Go to `pyignite3_path` folder
3. Execute `pip install -e .`

```bash
$ cd <pyignite3_path>
$ pip install -e .
```

This will install the repository version of `pyignite3` into your environment
in so-called “develop” or “editable” mode. You may read more about
[editable installs](https://pip.pypa.io/en/stable/reference/pip_install/#editable-installs)
in the `pip` manual.

Then run through the contents of `requirements` folder to install
the additional requirements into your working Python environment using
```
$ pip install -r requirements/<your task>.txt
```

You may also want to consult the `setuptools` manual about using `setup.py`.

## Testing
*NB!* It is recommended installing `pyignite3` in development mode.
Refer to [this section](#from-sources) for instructions.

Do not forget to install test requirements: 
```bash
$ pip install -r requirements/install.txt -r requirements/tests.txt
```

### Run basic tests
```bash
$ pytest
```
