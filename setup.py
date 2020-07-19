"""A setuptools based setup module.
See:
https://packaging.python.org/en/latest/distributing.html
https://github.com/pypa/sampleproject
"""

import ast
import re
from codecs import open
from os import path
from setuptools import find_packages, setup

cwd = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(cwd, "README.rst"), encoding="utf-8") as f:
    long_description = f.read()

_version_re = re.compile(r"VERSION\s+=\s+(.*)")

with open("ynab4_firefly_exporter/__init__.py") as f:
    version = str(ast.literal_eval(_version_re.search(f.read()).group(1)))

tests_require = [
    "pytest",
    "flake8",
    "mypy",
    "pytest-env",
    "ipdb",
    "coverage",
    "pytest-cov",
    "black",
]

setup(
    name="ynab4-firefly-exporter",
    version=version,
    description="YNAB 4 to Firefly iii Exporter",
    long_description=long_description,
    url="https://github.com/maroux/YNAB4-Firefly-iii-Exporter",
    author="Aniruddha Maru",
    license="MIT",
    maintainer="Aniruddha Maru",
    maintainer_email="aniruddhamaru@gmail.com",
    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        "Natural Language :: English",
        "Development Status :: 4 - Beta",
        "Intended Audience :: End Users/Desktop",
        "Topic :: Office/Business :: Financial :: Accounting",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: MIT License",
    ],
    python_requires=">=3.8",
    keywords="ynab ynab4 firefly fireflyiii ynab-exporter",
    # https://mypy.readthedocs.io/en/latest/installed_packages.html
    package_data={"ynab4_firefly_exporter": ["py.typed"]},
    packages=find_packages(exclude=["contrib", "docs", "tests", "tests.*"]),
    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    install_requires=["arrow", "dacite", "funcy", "ipdb", "requests[security]", "toml"],
    tests_require=tests_require,
    # List additional groups of dependencies here (e.g. development
    # dependencies). You can install these using the following syntax,
    # for example:
    # $ pip install -e .[dev,test]
    extras_require={"dev": ["flake8", "mypy"], "test": tests_require, "publish": ["wheel", "bumpversion", "twine"]},
    include_package_data=True,
    entry_points={"console_scripts": ["ynab4-firefly-exporter=ynab4_firefly_exporter.main:main"]},
)
