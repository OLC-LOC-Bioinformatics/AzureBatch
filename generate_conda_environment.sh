#!/bin/bash
# Generate Conda environment file from meta.yaml recipe

set -e  # Exit on any error

# Define input and output files
META_YAML="recipes/meta.yaml"
ENV_FILE="environment.yml"

echo "Generating environment file from recipe..."

# Create the environment file header
cat > $ENV_FILE << EOL
name: azure_batch
channels:
  - conda-forge
  - bioconda
  - defaults
dependencies:
EOL

# Extract run dependencies from meta.yaml and append to environment.yml
# This more specific grep/sed pattern extracts just the dependencies
grep -A 20 "run:" $META_YAML | grep -v "run:" | grep "=" | sed 's/^ *- /  - /g' | grep -v "^$" | grep -v "test:" -B 100 >> $ENV_FILE

# Add pip and the development package itself
cat >> $ENV_FILE << EOL
  - pip
  - pip:
    - -e .
EOL

echo "Environment file generated successfully:"
cat $ENV_FILE