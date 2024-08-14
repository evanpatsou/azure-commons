#!/bin/bash

# Create the pip.conf file with custom configurations
mkdir -p /databricks/driver/etc/pip

cat <<EOL > /databricks/driver/etc/pip/pip.conf
[global]
# Example: use a custom PyPI mirror
index-url = https://pypi.yourcompany.com/simple/
# Additional configurations
# trusted-host = pypi.yourcompany.com
# timeout = 60

[install]
# Example: install packages without using the cache
no-cache-dir = true
EOL

# Create a requirements.txt file with the desired packages
cat <<EOF > /databricks/driver/requirements.txt
azure-identity
databricks-connect
dbutils
pydantic
pydantic-settings
pandas
numpy
EOF

# Install Python packages using pip with the configured pip.conf and requirements.txt
pip install -r /databricks/driver/requirements.txt
