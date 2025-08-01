# pyignite_dbapi
Apache Ignite 3 DB API Driver.

## Prerequisites

- Python 3.9 or above (3.9, 3.10, 3.11, 3.12 and 3.13 are tested),
- Access to Ignite 3 node, local or remote.

## Installation

### From repository
This is a recommended way for users. If you only want to use the `pyignite_dbapi` module in your project, do:
```
$ pip install pyignite_dbapi
```

### From sources
This way is more suitable for developers, or if you install the client from zip archive.
1. Download and/or unzip Ignite 3 DB API Driver sources to `pyignite_dbapi_path`
2. Go to `pyignite_dbapi_path` folder
3. Execute `pip install -e .`

```bash
$ cd <pyignite_dbapi_path>
$ pip install -e .
```

This will install the repository version of `pyignite_dbapi` into your environment in so-called “develop” or “editable”
mode. You may read more about [editable installs](https://pip.pypa.io/en/stable/reference/pip_install/#editable-installs)
in the `pip` manual.

Then run through the contents of `requirements` folder to install the additional requirements into your working Python
environment using
```
$ pip install -r requirements/<your task>.txt
```

You may also want to consult the `setuptools` manual about using `setup.py`.

### *C extension*

The core of the package is a C++ extension. It shares the code with the Ignite C++ Client. The package is pre-built
for the most common platforms, but you may need to build it if your platform is not included.

Linux building requirements:
- GCC (and G++);
- CMake version >=3.18;
- OpenSSL (dev version of the package);
- Docker to build wheels;
- Supported versions of Python (3.9, 3.10, 3.11, 3.12 and 3.13).
  You can disable some of these versions, but you'd need to edit the script for that.

For building universal `wheels` (binary packages) for Linux, just invoke script `./scripts/create_distr.sh`.

Windows building requirements:
- MSVC 14.x, and it should be in path;
- CMake version >=3.18;
- OpenSSL (headers are required for the build);
- Supported versions of Python (3.9, 3.10, 3.11, 3.12 and 3.13).
  You can disable some of these versions, but you'd need to edit the script for that.

For building `wheels` for Windows, invoke script `.\scripts\BuildWheels.ps1` using PowerShell.
Make sure that your execution policy allows execution of scripts in your environment.
The script only works with Python distributions installed in a standard path, which is LOCALAPPDATA\Programs\Python.

Ready wheels will be located in `distr` directory.

### Updating from older version

To upgrade an existing package, use the following command:
```
pip install --upgrade pyignite_dbapi
```

To install the latest version of a package:

```
pip install pyignite_dbapi
```

To install a specific version:

```
pip install pyignite_dbapi==3.1.0
```

## Testing
*NB!* It is recommended installing `pyignite_dbapi` in development mode.
Refer to [this section](#from-sources) for instructions.

Remember to install test requirements:
```bash
$ pip install -r requirements/install.txt -r requirements/tests.txt
```

### Run basic tests
Running tests themselves:
```bash
$ pytest
```

## Documentation

Install documentation requirements:
```bash
$ pip install -r requirements/docs.txt
```

Generate documentation:

```bash
$ cd docs
$ make html
```

The resulting documentation can be found in `docs/_build/html`. If you want to open the documentation locally, you can
open the index of the documentation `docs/_build/html/index.html` using any modern browser.
