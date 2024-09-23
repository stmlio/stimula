#!/bin/bash

# build and install in the running docker container so Odoo uses the latest code.

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

# step 4: Copy the library to the install directory
rsync -av --delete $BUILD_DIR/dist $INSTALL_DIR/addons/

# Path to your custom library
CUSTOM_LIBRARY_PATH="/mnt/extra-addons"
ODOO_CONTAINER_NAME="docker_web_1"

# Check if the container is running
if [ "$(docker ps -q -f name=$ODOO_CONTAINER_NAME)" ]; then
    echo "Installing custom library in the running container..."
    docker exec -it $ODOO_CONTAINER_NAME bash -c "pip install --force-reinstall $CUSTOM_LIBRARY_PATH/dist/*.whl"
else
    echo "Container $ODOO_CONTAINER_NAME is not running."
fi
echo "Library built and installed successfully."

# restart the container
echo "Restarting the container..."
docker restart $ODOO_CONTAINER_NAME
echo "Container restarted successfully."
