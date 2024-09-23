#!/bin/bash

# Build and install in the docker project library so that PC can use it to step through the code when debugging.

# enable strict mode
set -euo pipefail

# Variables
BUILD_DIR="../../git/stimula"
INSTALL_DIR="../../odoo/docker"
VENV_DIR="venv3.10"
LIBRARY_NAME="stimula"

# Step 1: change to build directory and activate the virtual environment
cd ..
source venv/bin/activate

# Step 2: Remove the old build
rm -rf dist/ build/

# Step 3: Build the library
python setup.py sdist bdist_wheel
deactivate

# Step 4: Change to install directory and activate the virtual environment
pwd
cd $INSTALL_DIR
source $VENV_DIR/bin/activate

# Step 5: Install the library from the build directory
pip install --force-reinstall $BUILD_DIR/dist/*.whl

# Verify installation
python -c "from $LIBRARY_NAME import hello; print(hello())"

# Cleanup
deactivate

echo "Library built and installed successfully."
