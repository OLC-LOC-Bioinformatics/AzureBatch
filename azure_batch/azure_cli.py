#!/usr/bin/env python3

"""
Argument parser and class to run Azure batch methods to upload files to blob, as well as create, monitor, and delete
pools, jobs, and tasks. Code is based off of https://github.com/Azure-Samples/batch-python-quickstart
"""

# Standard imports
from argparse import (
    ArgumentParser,
    RawTextHelpFormatter
)
import datetime
import logging
import os
from pathlib import Path
import uuid

# Third party imports
from azure.batch import BatchServiceClient
import azure.batch.models as batchmodels
from azure.common.credentials import ServicePrincipalCredentials
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import (
    AccountSasPermissions,
    BlobServiceClient,
    BlobSasPermissions,
    generate_blob_sas,
    generate_container_sas,
    ResourceTypes
)

from azure_storage.methods import (
    create_container_client,
    validate_container_name
)
from dotenv import load_dotenv, dotenv_values

from azure_batch.methods import (
    add_tasks,
    copy_blobs_to_container,
    create_job,
    create_pool,
    download_files,
    log_output_resource_files,
    match_file_and_expression,
    parse_resource_file_list,
    parse_resource_input_pattern,
    prep_resource_files,
    print_batch_exception,
    read_bulk_input_pattern,
    read_command_file,
    Settings,
    upload_prep,
    wait_for_tasks_to_complete
)


__author__ = 'adamkoziol'


class AzureBatch:

    def main(self):
        # Use the blob client to create the container in Azure Storage if it doesn't yet exist.
        try:
            self.blob_service_client.create_container(self.container)
        except ResourceExistsError:
            pass

        # Collect the input files to be analysed.
        if self.upload_folder:
            logging.warning(f'Uploading files to {self.container}')
            upload_prep(
                upload_folder=self.upload_folder,
                blob_service_client=self.blob_service_client,
                container=self.container,
            )

        if self.input_file_pattern or self.bulk_input_file_pattern:
            # Read in the bulk resource file patterns
            if self.bulk_input_file_pattern:
                self.input_file_pattern = read_bulk_input_pattern(
                    bulk_input_file_pattern=self.bulk_input_file_pattern
                )

            # Validate the patterns, and add the container name to the destination path on the VM
            input_file_pattern_paths = parse_resource_input_pattern(
                input_file_pattern=self.input_file_pattern
            )
            # Set the name of the file to which resource file matches are to be written
            resource_file_list = os.path.join(self.path, 'resource_files.txt')
            # As additional matches are appended to the file, it must be deleted first
            if os.path.isfile(resource_file_list):
                os.remove(resource_file_list)

            logging.warning('Locating resource files in blob storage')
            # Find all the resource files in blob storage matching the resource patterns
            prep_resource_files(
                input_file_pattern=input_file_pattern_paths,
                blob_service_client=self.blob_service_client,
                resource_file_list=resource_file_list,
            )
            resource_files = parse_resource_file_list(
                resource_file_list=resource_file_list
            )
            # Match the retrieved file list with the file pattern
            resource_files_with_output = match_file_and_expression(
                resource_files=resource_files,
                input_file_pattern_paths=input_file_pattern_paths,
                container=self.container
            )
            # Copy all necessary files to the container
            logging.warning(f'Copying files to {self.container}')
            copy_blobs_to_container(
                container_name=self.container,
                resource_files_with_output=resource_files_with_output,
                settings=self.settings
            )

        # Set the credentials for creating a batch service client
        credentials = ServicePrincipalCredentials(
            client_id=self.settings.VM_CLIENT_ID,
            secret=self.settings.VM_SECRET,
            tenant=self.settings.VM_TENANT,
            resource="https://batch.core.windows.net/"
        )

        # Create the batch service client
        batch_client = BatchServiceClient(
            credentials,
            batch_url=self.settings.BATCH_ACCOUNT_URL
        )

        # If a unique ID was not provided, create an eight-digit hex to be used in creating pools/jobs/tasks
        if not self.unique_id:
            self.unique_id = uuid.uuid4().hex[:8]
            logging.warning(f'Using {self.unique_id} as the unique identifier')
        # Create variables to store the names for the pool, job, and task
        pool_id = f'{self.container}-{self.unique_id}-pool'
        job_id = f'{self.container}-{self.unique_id}-job'
        task_id = f'{self.container}-{self.unique_id}-task'
        # As there can be multiple tasks, add an integer to the task ID to keep them unique
        task_count = 0
        try:
            # Create the pool that will contain compute nodes to perform the analyses
            logging.warning(f'Creating pool {pool_id}')
            create_pool(
                batch_service_client=batch_client,
                pool_id=pool_id,
                vm_size=self.vm_size,
                settings=self.settings,
                container_name=self.container,
                mount_path=self.container
            )
            # Create the job that will run the tasks.
            logging.warning(f'Creating job {job_id} in pool {pool_id}')
            create_job(
                batch_service_client=batch_client,
                job_id=job_id,
                pool_id=pool_id
            )

            # Add the log files to the list of output files
            output_files = log_output_resource_files(
                blob_storage_service_client=self.blob_service_client,
                output_files=[],
                settings=self.settings,
                output_container_name=self.container
            )

            # Create a list to store the task(s)
            tasks = []

            # Create a task for each command in the file
            for cmd_num, cmd in enumerate(self.sys_call):
                # Do not specify resource_output_files until the final task
                if cmd_num < len(self.sys_call) - 1:
                    add_tasks(
                        task_id=f'{task_id}-{str(task_count)}',
                        tasks=tasks,
                        resource_input_files=[],
                        resource_output_files=[],
                        sys_call=cmd
                    )
                else:
                    add_tasks(
                        task_id=f'{task_id}-{str(task_count)}',
                        tasks=tasks,
                        resource_input_files=[],
                        resource_output_files=output_files,
                        sys_call=cmd
                    )
                task_count += 1
            # Add the task(s) to the job.
            batch_client.task.add_collection(
                job_id=job_id,
                value=tasks
            )

            # If this code is called by FoodPort, the task completion, file download, and pool/job cleanup will be
            # handled separately
            if self.worker:
                raise SystemExit

            # Pause execution until tasks reach Completed state.
            wait_for_tasks_to_complete(
                batch_service_client=batch_client,
                job_id=job_id,
                timeout=datetime.timedelta(hours=16)
            )

            # Download the requested files from the Azure storage
            if self.download_file_pattern:
                logging.warning('Downloading files from container')
                download_files(
                    container_name=self.container,
                    download_file_pattern=self.download_file_pattern,
                    path=self.path,
                    settings=self.settings
                )

            logging.warning("Success! All tasks reached the 'Completed' state within the specified timeout period.")

            # Print out some timing info
            end_time = datetime.datetime.now().replace(microsecond=0)
            elapsed_time = end_time - self.start_time
            logging.warning('Elapsed time: {elapsed_time}'.format(elapsed_time=elapsed_time))

        except batchmodels.BatchErrorException as err:
            print_batch_exception(err)
            raise

        finally:
            if not self.no_tidy:
                logging.warning('Cleaning up pool and job')
                # Clean up Batch resources
                batch_client.job.delete(job_id)
                batch_client.pool.delete(pool_id)

    def __init__(self, command_file, vm_size, settings, container, path, upload_folder=None,
                 input_file_pattern=None, bulk_input_file_pattern=None, download_file_pattern=None,
                 unique_id=None, worker=True, no_tidy=False):

        # Use datetime.datatime to set the current time. Will be used to calculate timeouts
        self.start_time = datetime.datetime.now().replace(microsecond=0)

        logging.warning('Beginning batch submission process')

        # Read in the command(s)
        self.sys_call = read_command_file(
            command_file=command_file
        )
        self.settings = settings
        # Create the blob client, for use in obtaining references to blob storage containers and uploading
        # files to containers.
        self.blob_service_client = BlobServiceClient(
            account_url=f'https://{settings.AZURE_ACCOUNT_NAME}.blob.core.windows.net/',
            credential=self.settings.AZURE_ACCOUNT_KEY
        )
        # Validate the supplied container name
        self.container = validate_container_name(
            container_name=container
        )
        logging.warning(f'Container name {self.container} is valid')
        self.upload_folder = upload_folder
        self.input_file_pattern = input_file_pattern
        self.bulk_input_file_pattern = bulk_input_file_pattern
        self.path = path
        self.unique_id = unique_id
        self.vm_size = vm_size
        self.worker = worker
        self.download_file_pattern = download_file_pattern
        self.no_tidy = no_tidy


def cli():
    parser = ArgumentParser(
        description='Run workflows in batch VMs on Azure',
        formatter_class=RawTextHelpFormatter
    )
    parser.add_argument(
        '-c', '--container',
        metavar='container',
        required=True,
        type=str,
        help="""
        Name of container for input files to be used in the analyses. This 
        container will be mounted to the VM using blobfuse, so all input files 
        must either already be present, uploaded (--upload), or copied (-input 
        or -bulk_input) to this container.
        """
    )
    parser.add_argument(
        '-cmd', '--cmd',
        metavar='system_call_file',
        required=True,
        type=str,
        help="""
        Name and path of file containing system call(s) to run in task(s) (one 
        per line). The command(s) must include any environment activation steps,
        e.g.:

        `source $CONDA/activate /envs/cowbat && assembly_pipeline.py -s 
        $AZ_BATCH_NODE_MOUNTS_DIR/container-name -r /databases/0.5.0.23`

        Note that the $CONDA directory is assumed to be /usr/bin/miniconda/bin 
        and the $AZ_BATCH_NODE_MOUNTS_DIR is a default environment variable 
        where all mount directories reside. For Ubuntu, this location is 
        /mnt/batch/tasks/fsmounts.
        """
    )
    parser.add_argument(
        '-s', '--settings',
        metavar='settings',
        required=True,
        help="""
        Name and path of file with the following Azure credentials: 
        AZURE_ACCOUNT_NAME=blob storage account name
        AZURE_ACCOUNT_KEY=blob storage account key
        BATCH_ACCOUNT_NAME=azure batch account name
        BATCH_ACCOUNT_URL=azure batch account URL
        BATCH_ACCOUNT_KEY=azure batch account key
        VM_IMAGE=resource ID of VM image on Azure
        VM_CLIENT_ID=user:name extracted from `az account list`
        VM_SECRET=extracted from `az vm secret list --name MyVirtualMachine 
        --resource-group MyResourceGroup`
        VM_TENANT=tenantID extracted from az account list
        """
    )
    parser.add_argument(
        '-vm', '--vm_size',
        metavar='vm_size',
        default='Standard_D32s_v3',
        choices=['Standard_D32s_v3', 'Standard_D16s_v3', 'Standard_D8s_v3', 
                 'Standard_D4ds_v5'],
        help="""
        Size of VM to use. Default is 'Standard_D32s_v3'
        """
    )
    parser.add_argument(
        '-p', '--path',
        metavar='path',
        default=os.getcwd(),
        type=str,
        help="""
        Name and path of folder into which files from tasks are to be 
        downloaded. If not provided, the current working directory will be used.
        """
    )
    parser.add_argument(
        '-u', '--upload_folder',
        metavar='upload',
        type=str,
        help="""
        Path of folder containing files to upload for the analyses.
        """
    )

    input_group = parser.add_mutually_exclusive_group()
    input_group.add_argument(
        '-input', '--input_file_pattern',
        metavar='input_file_pattern',
        action='append',
        nargs='+',
        help="""
        Pattern to use to specify which file(s)/folder(s) to copy to the 
        container (--container), as well as any required folder structure. The 
        wildcard character * can be used to specify multiple files/folders. By 
        default, the files will be placed in the root of the container, so you 
        only need to specify the destination if the files need to be in a 
        subdirectory. 
        
        The general format of the argument is: `-input container_name/file_name 
        destination_folder`. e.g.:

            `-input sequence_data/2022-SEQ-1399.fasta sequences` <- this file 
            will be placed in the 'sequences' subdirectory of the supplied 
            --container

            `-input sequence_data/2022-SEQ-1399.fasta`  <- this file will be 
            placed in the --container

            `-input sequence_data/*.fasta sequences` <- all the .fasta files 
            from the sequence_data container will be copied to the --container
            
            `-input sequence_data/escherichia/ sequences` <- all files in the 
            'escherichia' folder in the sequence_data container will be copied 
            to the 'sequences' folder in the --container (note that the 
            trailing slash is required)
            
            `-input sequence_data/escherichia/* sequences/verotoxin` <- 
            this allows nesting within the destination folder
        
        The -input argument can also be provided multiple times. e.g.:

            `-input sequence_data/*.fasta sequences -input 
            targets/verotoxin_targets.fasta targets`

        This argument is mutually exclusive with --bulk_input_file_pattern.
        """
    )
    input_group.add_argument(
        '-bulk_input', '--bulk_input_file_pattern',
        metavar='bulk_input_file_pattern',
        type=str,
        help="""
        Name and path of a text file with the required file(s)/folder(s) to 
        copy to the --container, as well as their destination folder. Example 
        arguments are provided in the --input_file_pattern option above. 
        This argument is mutually exclusive with --input_file_pattern.
        """
    )
    parser.add_argument(
        '-download', '--download_file_pattern',
        metavar='download_file_pattern',
        action='append',
        nargs='+',
        help="""
        Pattern to use to specify which file(s)/folder(s) to download from blob 
        storage following successful completion of the analyses. Specify a 
        file name, e.g. 'log.txt', or a folder, e.g. 'reports/'. (Note the 
        trailing slash is mandatory in order for the program to recognise a 
        folder vs a file.) This argument can be supplied multiple times. e.g 
        `-download log.txt -download error.txt -download reports` 
        Files will be downloaded to the location specified by the -path 
        argument.
        """
    )
    parser.add_argument(
        '-unique_id', '--unique_id',
        metavar='unique_id',
        help="""
        Provide an identifier to append to pool/job/task names. By default a 
        random eight-digit hash is added to ensure that no collisions occur. 
        However, when this code is called from FoodPort, the primary key for 
        the model of the analysis will be used. Using this argument can 
        duplicate this functionality.
        """
    )
    parser.add_argument(
        '-v', '--verbosity',
        choices=['debug', 'info', 'warning', 'error', 'critical'],
        metavar='VERBOSITY',
        default='warning',
        help="""
        Set the logging level. Options are debug, info, warning, error, and
        critical. Default is info.
        """
    )
    parser.add_argument(
        '-no_tidy', '--no_tidy',
        action='store_true',
        help="""
        Do not automatically delete pools/jobs/tasks when the script errors or
        completes. Useful for debugging VM. PLEASE REMEMBER TO CLEAN EVERYTHING
        UP MANUALLY.
        """
    )
    arguments = parser.parse_args()
    logging.basicConfig(
        level=arguments.verbosity.upper(),
        format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    # Load the environment variables from file
    assert os.path.isfile(arguments.settings)
    dotenv_path = Path(arguments.settings)
    load_dotenv(dotenv_path=dotenv_path)
    settings_dict = dotenv_values(dotenv_path)
    local_settings = Settings(settings=settings_dict)

    azure_batch = AzureBatch(
        command_file=arguments.cmd,
        vm_size=arguments.vm_size,
        settings=local_settings,
        container=arguments.container,
        path=arguments.path,
        upload_folder=arguments.upload_folder,
        input_file_pattern=arguments.input_file_pattern,
        bulk_input_file_pattern=arguments.bulk_input_file_pattern,
        download_file_pattern=arguments.download_file_pattern,
        unique_id=arguments.unique_id,
        worker=False,
        no_tidy=arguments.no_tidy
    )
    azure_batch.main()
    raise SystemExit


if __name__ == '__main__':
    cli()
