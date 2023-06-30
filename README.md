# AzureBatch

AzureBatch is a Python package for creating batch VMs based upon previously created images for *ad hoc* bioinformatics analysis at OLC.

## Installation

Until AzureBatch is available in the olcbioinformatics Conda channel, it can be installed using the following steps:

```bash
# Create and activate a Conda environment
conda create -n azurebatch -c olcbioinformatics azure_storage python-dotenv azure-batch tqdm
conda activate azurebatch
# Clone the AzureBatch Git repo
git clone https://github.com/OLC-LOC-Bioinformatics/AzureBatch.git
cd AzureBatch/
# Install the package in dev mode
pip intall -e .
```

## Quick usage

To create a new batch VM, you must first have the following on your local computer:

- A file containing the command(s) to run after the VM has been created (i.e. `cmd.sh`).
- A file containing the Azure credentials for creating the VM and the remote path to the VM image (i.e. `settings.txt`).

To create a batch VM, mount a blob container named 'container001', and run your commands specified within `cmd.sh`:

```bash
python AzureBatch/azurebatch/azure_cli.py \
    -c container001 \
    -cmd cmd.sh \
    -s settings.txt
```

To upload all files within a local directory `data/` into the root directory of the mounted blob container, and then run your commands:

```bash
python AzureBatch/azurebatch/azure_cli.py \
    -c container001 \
    -cmd cmd.sh \
    -s settings.txt \
    -u data/
```

**Note:** To upload files to a mounted blob container, you must be connected to the CFIA network.
