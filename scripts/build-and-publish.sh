#!/bin/bash

# Build and install in the docker project library so that PC can use it to step through the code when debugging.

# enable strict mode
set -euo pipefail

# change to build directory
cd ..

# remove old distribution
rm dist/*

# create distribution
python3 setup.py sdist bdist_wheel

#upload to pypi (pypi token in lastpass)
twine upload dist/*

echo "Library built and published successfully."
