#!/usr/bin/env python3

"""
Methods to upload files as required, create and delete pools, jobs, and
tasks, and, finally, download files for Azure batch analyses
"""

# Standard imports
import datetime
from glob import glob
import logging
import os
from pathlib import Path
import re
import sys
import time


# Third party imports
from azure.batch import BatchServiceClient
import azure.batch.models as batchmodels
from azure.core.exceptions import (
    ResourceExistsError,
    ResourceNotFoundError
)
from azure.identity import ClientSecretCredential
from azure.mgmt.compute import ComputeManagementClient

from azure.storage.blob import (
    AccountSasPermissions,
    BlobServiceClient,
    BlobSasPermissions,
    generate_blob_sas,
    generate_container_sas
)
from azure_storage.azure_download import AzureDownload
from azure_storage.azure_list import AzureList
from azure_storage.azure_move import AzureMove
from azure_storage.methods import (
    create_container_client
)
from tqdm import tqdm

__author__ = 'adamkoziol'


class TqdmUpTo(tqdm):
    """
    A subclass of tqdm providing an `update_to` method.

    The `update_to` method is used with the progress of an upload or download,
    where the total size and the currently transferred size are known.

    Attributes:
        total (int): The total size of the operation, set by `update_to`.
    """

    def update_to(self, response):
        """
        Updates the progress bar based on 'upload_stream_current' and
        'data_stream_total' values in the context of the given response.

        Args:
            response (Response): The response object that includes the context
                with 'upload_stream_current' and 'data_stream_total' values.

        Returns:
            int: The difference between the current transferred size and the
                previously transferred size. This is passed to `tqdm.update`.
        """
        # There's also a 'download_stream_current'
        current = response.context['upload_stream_current']
        total = response.context['data_stream_total']
        if total is not None:
            self.total = total
        if current is not None:
            return self.update(current - self.n)


class Settings:
    """
    A class used to represent the settings for Azure and Batch accounts.

    ...

    Attributes
    ----------
    azure_account_name : str
        The name of the Azure account.
    azure_account_key : str
        The key for the Azure account.
    batch_account_url : str
        The URL for the Batch account.
    batch_account_subnet : str
        The subnet for the Batch account.
    vm_image : str
        The image for the Virtual Machine, depends on the analysis type.
    node_agent_sku_id : str
        The SKU ID for the node agent, depends on the analysis type.
    vm_secret : str
        The secret for the Virtual Machine.
    vm_tenant : str
        The tenant for the Virtual Machine.

    Methods
    -------
    __init__(self, settings: dict, analysis_type: str) -> None:
        Initializes the Settings object with the provided settings.
    """

    def __init__(self, settings, analysis_type):
        """
        Initializes the Settings object with the provided settings.

        Parameters
        ----------
        settings : dict
            A dictionary containing the settings for Azure and Batch accounts.
        analysis_type : str
            The type of analysis to be performed. This determines the VM
            image and node agent SKU ID.
        """
        self.azure_account_name = settings['AZURE_ACCOUNT_NAME']
        self.azure_account_key = settings['AZURE_ACCOUNT_KEY']
        self.batch_account_url = settings['BATCH_ACCOUNT_URL']
        self.batch_account_subnet = settings['BATCH_ACCOUNT_SUBNET']
        if analysis_type == 'COWBAT':
            self.vm_image = settings['VM_IMAGE']
            self.node_agent_sku_id = settings['COWBAT_NODE_AGENT_SKU']
        elif analysis_type == 'AmpliSeq':
            self.vm_image = settings['AMPLISEQ_IMAGE']
            self.node_agent_sku_id = settings['AMPLISEQ_NODE_AGENT_SKU']
        elif analysis_type == 'COWSNPhR':
            self.vm_image = settings['COWSNPHR_IMAGE']
            self.node_agent_sku_id = settings['COWSNPHR_NODE_AGENT_SKU']
        self.vm_secret = settings['VM_SECRET']
        self.vm_tenant = settings['VM_TENANT']
        self.vm_client_id = settings['VM_CLIENT_ID']


def print_batch_exception(
        batch_exception: batchmodels.BatchErrorException):
    """
    Prints the contents of the specified Batch exception.
    :param batch_exception: batchmodels.BatchErrorException
    """
    print('-------------------------------------------')
    print('Exception encountered:')
    if batch_exception.error and \
            batch_exception.error.message and \
            batch_exception.error.message.value:
        print(batch_exception.error.message.value)
        if batch_exception.error.values:
            print()
            for message in batch_exception.error.values:
                print(f'{message.key}:\t{message.value}')
    print('-------------------------------------------')


def upload_file_to_container(
        blob_storage_service_client: BlobServiceClient,
        container_name: str,
        file_path: str,
        upload_folder: str,
        overwrite=False):
    """
    Uploads a local file to an Azure Blob storage container.
    :param blob_storage_service_client: A blob service client.
    :param str container_name: The name of the Azure Blob storage container.
    :param str file_path: The local path to the file.
    :param str upload_folder: User-supplied name and path of folder containing
    files to upload
    :param bool overwrite: Boolean of whether previous blobs will be
    overwritten. Default is False
    """
    # Calculate the relative path between the file_path and the upload folder
    # e.g. tests/files/test.txt with upload folder tests/files/ will yield
    # tests.txt, while tests/files/uploads/test.txt will yield uploads/test.txt
    blob_name = str(Path(file_path).relative_to(upload_folder))

    # Create a blob client for the blob in the container
    blob_client = blob_storage_service_client.get_blob_client(
        container_name,
        blob_name
    )

    # Upload the file to storage. Don't overwrite any previous versions
    try:
        # Progress bar with upload
        with TqdmUpTo(
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
                miniters=1,
                desc=blob_name) as t:
            with open(file_path, "rb") as data:
                blob_client.upload_blob(
                    data,
                    overwrite=overwrite,
                    raw_response_hook=t.update_to,
                    connection_timeout=1200
                    )
            t.total = t.n
        logging.warning("File '%s' successfully uploaded.", blob_name)
    except ResourceExistsError:
        logging.warning("File '%s' already exists. Skipping...", blob_name)


def generate_sas_url(
        account_name: str,
        account_domain: str,
        container_name: str,
        blob_name: str,
        sas_token: str) -> str:
    """
    Generates and returns a SAS URL for accessing a file in Azure storage
    :param str account_name: Name of Azure storage account
    :param str account_domain: Domain of Azure storage account
    :param str container_name: Name of Azure storage container
    :param str blob_name: Name of blob in Azure storage container
    :param str sas_token: SAS token created by
    azure.storage.blob.generate_blob_sas
    :return str: Formatted SAS URL
    """
    # Create the SAS URL using the supplied variables
    if blob_name:
        return (
            f'https://{account_name}.{account_domain}/'
            f'{container_name}/{blob_name}?{sas_token}'
        )
    return (
        f'https://{account_name}.{account_domain}/'
        f'{container_name}?{sas_token}'
    )


def create_pool(
    batch_service_client: BatchServiceClient,
    pool_id: str,
    vm_size: str,
    container_name: str,
    mount_path: str,
    settings: Settings
):
    """
    Creates a pool of compute nodes with the specified OS settings.
    :param batch_service_client: A Batch service client.
    :param str pool_id: An ID for the new pool
    :param str vm_size: The size of the VM to use.
    :param str container_name: The name of the Azure Blob storage container.
    :param str mount_path: The relative path the container will be mounted in
    the VM. $AZ_BATCH_NODE_MOUNTS_DIR is where
    all mount directories reside, so the relative path is the folder to use
    in that directory
    :param Settings settings: Class containing environment variables
    """
    # Create VM configuration with conditional TrustedLaunch
    image_ref = batchmodels.ImageReference(
        virtual_machine_image_id=settings.vm_image,
    )

    # Check if the image requires or supports TrustedLaunch
    security_requirements = check_image_security_requirements(
        settings=settings
    )

    # Configure VM based on security requirements
    if security_requirements["recommended_security_profile"]:

        # Set the security type
        security_type = security_requirements[
            'recommended_security_profile'
        ][
            'security_type'
        ]

        # Create the VM configuration object with the security type
        vm_config = batchmodels.VirtualMachineConfiguration(
            image_reference=image_ref,
            node_agent_sku_id=settings.node_agent_sku_id,
            security_profile=batchmodels.SecurityProfile(
                security_type=security_type
            )
        )
        logging.info(
            "Configuring VM with security profile: %s", security_type
        )
    else:

        # Create the Vm configuration object without the security type
        vm_config = batchmodels.VirtualMachineConfiguration(
            image_reference=image_ref,
            node_agent_sku_id=settings.node_agent_sku_id
        )
        logging.info(
            "VM configured without security profile - "
            "TrustedLaunch not supported/required"
        )

    # Create a new pool of compute nodes
    new_pool = batchmodels.PoolAddParameter(
        id=pool_id,
        virtual_machine_configuration=vm_config,
        vm_size=vm_size,
        network_configuration=batchmodels.NetworkConfiguration(
            subnet_id=settings.batch_account_subnet,
            public_ip_address_configuration=(
                batchmodels.PublicIPAddressConfiguration(
                    provision='noPublicIPAddresses'
                )
            )
        ),
        target_dedicated_nodes=1,
        mount_configuration=[
            batchmodels.MountConfiguration(
                azure_blob_file_system_configuration=(
                    batchmodels.AzureBlobFileSystemConfiguration(
                        account_name=settings.azure_account_name,
                        account_key=settings.azure_account_key,
                        container_name=container_name,
                        relative_mount_path=mount_path,
                        blobfuse_options=(
                            '--file-cache-timeout-in-seconds=240 '
                            '--attr-cache-timeout=240 '
                            '--entry-timeout=240 '
                            '--negative-timeout=120 '
                            '--tmp-path=/mnt/resource/blobfuse2_cache '
                            '--cache-size-mb=2048 '
                            '--log-level=LOG_WARNING '
                            '--use-attr-cache=true '
                            '--max-concurrency=64 '
                        )
                    )
                )
            )
        ]
    )
    batch_service_client.pool.add(new_pool)


def check_image_security_requirements(
    *,  # Enforce keyword arguments
    settings: Settings
):
    """Examines an Azure VM image for security requirements.

    Determines if the VM image supports or requires TrustedLaunch security
    features and extracts the relevant security profile information.

    :param Settings settings: Class containing environment variables

    Returns:
        dict: Security requirements information with keys:
            - supports_trusted_launch: If image supports TrustedLaunch
            - requires_trusted_launch: If image requires TrustedLaunch
            - security_type: Security type if specified
            - recommended_security_profile: Recommended profile settings
    """
    # Use image_id from settings
    image_id = settings.vm_image

    # Parse the image ID to extract subscription ID and other components
    pattern = (
        r'/subscriptions/([^/]+)/resourceGroups/([^/]+)/providers/'
        r'Microsoft.Compute/galleries/([^/]+)/images/([^/]+)/'
        r'versions/([^/]+)'
    )
    match = re.match(pattern, image_id)

    if not match:
        logging.warning(
            "Invalid image ID format: %s, defaulting to recommended settings",
            image_id
        )
        return {
            "supports_trusted_launch": True,
            "recommended_security_profile": {"security_type": "trustedLaunch"}
        }

    subscription_id, resource_group, gallery_name, image_name, _ = \
        match.groups()

    # Create credential object using settings
    credential = ClientSecretCredential(
        tenant_id=settings.vm_tenant,
        client_id=settings.vm_client_id,
        client_secret=settings.vm_secret
    )

    try:
        # Create compute client
        compute_client = ComputeManagementClient(credential, subscription_id)

        # Get the image definition details
        image_definition = compute_client.gallery_images.get(
            resource_group,
            gallery_name,
            image_name
        )

        # Default security info
        security_info = {
            "supports_trusted_launch": False,
            "requires_trusted_launch": False,
            "security_type": None,
            "recommended_security_profile": None
        }

        # Check if the image has security profile information
        if hasattr(image_definition, "features") and image_definition.features:
            for feature in image_definition.features:
                if (feature.name == "SecurityType" and
                        "TrustedLaunch" in feature.value):
                    security_info["supports_trusted_launch"] = True
                    security_info["security_type"] = "TrustedLaunch"
                    break

        # Check OS specifics - Ubuntu 22.04+ supports TrustedLaunch
        if hasattr(image_definition, "os_type") and \
                image_definition.os_type == "Linux":
            if "ubuntu" in image_name.lower() and any(
                    v in image_name.lower() for v in ["22.04", "24.04"]):
                security_info["supports_trusted_launch"] = True

        # Set recommended security profile based on findings
        if security_info["supports_trusted_launch"]:
            security_info["recommended_security_profile"] = {
                "security_type": "trustedLaunch"
            }

        logging.info(
            "Image %s security analysis: %s",
            image_name,
            security_info
        )
        return security_info

    except Exception as exc:
        logging.warning(
            "Error checking security requirements: %s, using defaults",
            str(exc)
        )
        # For newer Ubuntu, default to TrustedLaunch
        if "ubuntu" in image_id.lower() and any(
                v in image_id.lower() for v in ["22.04", "24.04"]):
            return {
                "supports_trusted_launch": True,
                "recommended_security_profile": {
                    "security_type": "trustedLaunch"
                }
            }
        return {
            "supports_trusted_launch": False,
            "recommended_security_profile": None
        }


def create_job(
        batch_service_client: BatchServiceClient,
        job_id: str,
        pool_id: str):
    """
    Creates a job with the specified ID, associated with the specified pool.
    :param batch_service_client: A Batch service client.
    :param str job_id: The ID for the job.
    :param str pool_id: The ID for the pool.
    """
    # Create a job linked to the pool
    job = batchmodels.JobAddParameter(
        id=job_id,
        pool_info=batchmodels.PoolInformation(pool_id=pool_id))

    batch_service_client.job.add(job)


def add_tasks(
        task_id: str,
        tasks: list,
        resource_input_files: list,
        resource_output_files: list,
        sys_call: str) -> list:
    """
    Adds a task for each input file in the collection to the specified job.
    :param str task_id: Unique ID for the task
    :param list tasks: List of tasks to perform
    :param list resource_input_files: A collection of input files to add to
    the task
    :param list resource_output_files: List of azure.batch.models.OutputFiles
    :param str sys_call: The system call to perform
    :return list tasks: Task list with appended task
    """

    # Give a sixteen-hour timeout for the task
    # https://docs.microsoft.com/en-us/python/api/azure-batch/azure.batch.models.taskconstraints?view=azure-python
    task_constraints = batchmodels.TaskConstraints(max_wall_clock_time="PT16H")
    # Since the system command does not run under a shell, prepend /bin/bash
    # -c to the command to allow for environment
    # variable expansion
    command = f'/bin/bash -c \"{sys_call}\"'
    # Run the task as an auto-user with elevated access. Necessary for using
    # blobfuse filesystems
    # https://learn.microsoft.com/en-us/azure/batch/batch-user-accounts#run-a-task-as-an-auto-user-with-elevated-access
    user = batchmodels.UserIdentity(
        auto_user=batchmodels.AutoUserSpecification(
            elevation_level=batchmodels.ElevationLevel.admin,
            scope=batchmodels.AutoUserScope.pool)
    )
    tasks.append(batchmodels.TaskAddParameter(
        id=task_id,
        constraints=task_constraints,
        command_line=command,
        resource_files=resource_input_files,
        output_files=resource_output_files,
        user_identity=user,
        environment_settings=[
            batchmodels.EnvironmentSetting(
                name='CONDA',
                value='/usr/bin/miniconda/bin'
            ),
        ])
    )
    return tasks


def prep_output_container(
        output_container_name: str,
        settings: Settings,
        blob_storage_service_client: BlobServiceClient) -> str:
    """
    Create the container to receive files following task completion/success.
    Create a SAS URL for the container in order to initialise
    batchmodels.OutputFile objects
    :param str output_container_name: Name of container in Azure storage into
    which output files are to be uploaded
    :param Settings settings: Class containing environment variables
    :param BlobServiceClient blob_storage_service_client: BlobServiceClient
    :return str sas_url: SAS URL of output container
    """
    try:
        blob_storage_service_client.create_container(
            name=output_container_name
        )
    except ResourceExistsError:
        pass
    sas_token = generate_container_sas(
        account_name=settings.azure_account_name,
        container_name=output_container_name,
        account_key=settings.azure_account_key,
        permission=AccountSasPermissions(read=True, write=True),
        expiry=datetime.datetime.now(datetime.timezone.utc) +
        datetime.timedelta(hours=24)
    )
    sas_url = generate_sas_url(
        account_name=settings.azure_account_name,
        account_domain='blob.core.windows.net',
        container_name=output_container_name,
        blob_name=str(),
        sas_token=sas_token
    )
    return sas_url


def prepare_output_resource_files(
        blob_storage_service_client: BlobServiceClient,
        output_item: str,
        output_files: list,
        settings: Settings,
        output_container_name: str) -> list:
    """
    Create batchmodels.OutputFile object(s) for desired file/folder to be
    uploaded following task success
    :param BlobServiceClient blob_storage_service_client: BlobServiceClient
    :param str output_item: Name of file/folder to upload
    :param list output_files: List of batchmodels.OutputFile objects for
    files to be uploaded
    :param Settings settings: Class containing environment variables
    :param str output_container_name: Name of container in Azure storage into
    which output files are to be uploaded
    :return list output_files: Populated with batchmodels.OutputFile objects
    """
    # Create the output container (if necessary), and create a SAS URL
    # for the container
    sas_url = prep_output_container(
        output_container_name=output_container_name,
        settings=settings,
        blob_storage_service_client=blob_storage_service_client
    )
    # If the output_item ends with a /, it is a folder, so it needs to be
    # processed recursively
    if output_item.endswith('/'):
        # If all files and folders are to be retrieved, the output_item is
        # a /. Remove the / to not confuse the patterns
        output_item = output_item if output_item != '/' else ''
        # Create a batchmodels.OutputFile object
        output_files.append(batchmodels.OutputFile(
            # Match all files
            file_pattern=output_item + '*',
            # Set the file output destination
            destination=batchmodels.OutputFileDestination(
                # Set the container output details
                container=batchmodels.OutputFileBlobContainerDestination(
                    container_url=sas_url,
                    path=os.path.split(output_item)[0]
                )
            ),
            # Upload the file when the task is successful
            upload_options=batchmodels.OutputFileUploadOptions(
                upload_condition=(
                    batchmodels.OutputFileUploadCondition.task_success
                )
            )
        ))
        output_files.append(batchmodels.OutputFile(
            # ** specifies any folder
            file_pattern=os.path.join(output_item, '**', '*'),
            destination=batchmodels.OutputFileDestination(
                container=batchmodels.OutputFileBlobContainerDestination(
                    container_url=sas_url,
                    # Remove the ** prepended to the file name
                    path=os.path.split(output_item)[0].replace('**', '')
                )
            ),
            upload_options=batchmodels.OutputFileUploadOptions(
                upload_condition=(
                    batchmodels.OutputFileUploadCondition.task_success
                )
            )
        ))
    # Otherwise, a single batchmodels.Output object is created for the
    # specified file
    else:
        output_files.append(batchmodels.OutputFile(
            file_pattern=output_item,
            destination=batchmodels.OutputFileDestination(
                container=batchmodels.OutputFileBlobContainerDestination(
                    container_url=sas_url,
                    path=os.path.split(output_item)[0])),
            upload_options=batchmodels.OutputFileUploadOptions(
                upload_condition=(
                    batchmodels.OutputFileUploadCondition.task_success
                )
            )
        ))

    return output_files


def log_output_resource_files(
        blob_storage_service_client: BlobServiceClient,
        output_files: list,
        settings: Settings,
        output_container_name: str) -> list:
    """
    Create batchmodels.OutputFile object(s) for log files following
    task completion
    :param BlobServiceClient blob_storage_service_client: BlobServiceClient
    :param list output_files: List of batchmodels.OutputFile objects for files
    to be uploaded
    :param Settings settings: Class containing environment variables
    :param str output_container_name: Name of container in Azure storage into
    which output files are to be uploaded
    :return list output_files: Populated with batchmodels.OutputFile objects
    """
    # Create the output container (if necessary), and create a SAS URL for
    # the container
    sas_url = prep_output_container(
        output_container_name=output_container_name,
        settings=settings,
        blob_storage_service_client=blob_storage_service_client
    )

    # Add stdout and stderr.txt log files to the Azure container. This is
    # done even if task isn't successful
    output_files.append(
        batchmodels.OutputFile(
            file_pattern=os.path.join('$AZ_BATCH_TASK_DIR', 'stderr.txt'),
            destination=batchmodels.OutputFileDestination(
                container=batchmodels.OutputFileBlobContainerDestination(
                    container_url=sas_url,
                    path='azure_stderr.txt'
                )
            ),
            upload_options=batchmodels.OutputFileUploadOptions(
                upload_condition=(
                    batchmodels.OutputFileUploadCondition.task_completion
                )
            )
        )
    )
    output_files.append(
        batchmodels.OutputFile(
            file_pattern=os.path.join('$AZ_BATCH_TASK_DIR', 'stdout.txt'),
            destination=batchmodels.OutputFileDestination(
                container=batchmodels.OutputFileBlobContainerDestination(
                    container_url=sas_url,
                    path='azure_stdout.txt'
                )
            ),
            upload_options=batchmodels.OutputFileUploadOptions(
                upload_condition=(
                    batchmodels.OutputFileUploadCondition.task_completion
                )
            )
        )
    )
    return output_files


def wait_for_tasks_to_complete(
        batch_service_client: BatchServiceClient,
        job_id: str,
        timeout: datetime.timedelta):
    """
    Returns when all tasks in the specified job reach the Completed state.
    :param batch_service_client: A Batch service client.
    :param job_id: The id of the job whose tasks should be monitored.
    :param timeout: The duration to wait for task completion. If all tasks in
    the specified job do not reach Completed state within this time period,
    an exception will be raised.
    """
    # Set the timeout
    timeout_expiration = datetime.datetime.now() + timeout

    logging.warning(
        "Monitoring all tasks for 'Completed' state, timeout in %s...",
        timeout
    )

    # While the current time is under the timeout, allow the tasks to proceed
    while datetime.datetime.now() < timeout_expiration:
        # Add a dot to show that the script is still running
        print('.', end='')
        sys.stdout.flush()

        # Get a list of all the tasks for the job
        tasks = batch_service_client.task.list(job_id)
        # Create a list of all tasks that are not in the 'Completed' state
        incomplete_tasks = [
            task for task in tasks if task.state !=
            batchmodels.TaskState.completed
        ]
        # If all the tasks are complete, break the while loop
        if not incomplete_tasks:
            # Print an empty line to exit the line of dots
            print()
            return True
        # Check every second
        time.sleep(1)
    # Print an empty line to exit the line of dots
    print()
    # Raise a RuntimeError if the tasks take too long
    raise RuntimeError(
        "ERROR: Tasks did not reach 'Completed' state within timeout "
        "period of " + str(timeout)
    )


def upload_prep(
        upload_folder: str,
        blob_service_client: BlobServiceClient,
        container: str):
    """
    Assert that the user-supplied upload folder exists, find all files in that
    folder, and upload them to blob storage
    :param str upload_folder: User-supplied name and path of folder
    containing files to upload
    :param blob_service_client: BlobServiceClient
    :param str container: User-supplied name of container to which files are
    to be uploaded
    """
    # Assert that the supplied upload folder exists
    try:
        assert os.path.isdir(upload_folder)
    except AssertionError as exc:
        logging.error(
            "Could not locate the supplied folder containing files to upload: "
            "%s", upload_folder
        )
        raise SystemExit from exc

    # Use glob recursively to find all the files in the supplied upload folder
    input_file_paths = sorted(
        glob(
            os.path.join(upload_folder, '**', '*'), recursive=True
        )
    )

    logging.debug(input_file_paths)

    # Upload the data files.
    for input_file_path in input_file_paths:
        # Ignore directories
        if os.path.isdir(input_file_path):
            continue
        # Upload the file to the container
        upload_file_to_container(
            blob_storage_service_client=blob_service_client,
            container_name=container,
            file_path=input_file_path,
            upload_folder=upload_folder
        )


def read_bulk_input_pattern(bulk_input_file_pattern: str) -> list:
    """
    Read the supplied file of input file patterns into a list
    :param str bulk_input_file_pattern: Name and path of a file containing
    resource file input patterns
    :return: list file_patterns: File patterns extracted from file
    """
    # Ensure that the file actually exists
    try:
        assert os.path.isfile(bulk_input_file_pattern)
    except AssertionError as exc:
        logging.error(
            "Could not locate file containing bulk input file patterns: %s",
            bulk_input_file_pattern
        )
        raise SystemExit from exc

    # Create a list to store the parsed patterns
    file_patterns = []
    with open(bulk_input_file_pattern, 'r', encoding='utf-8') as bulk_file:
        for line in bulk_file:
            # Split the line on whitespace
            pattern = line.rstrip().split()
            # Add the list to the list of patterns
            file_patterns.append(pattern)
    return file_patterns


def parse_resource_input_pattern(
        input_file_pattern: list) -> list:
    """
    Ensure that the resource file patterns have the correct format. Add the
    container name to the path in the VM
    :param list input_file_pattern: List of lists of [file pattern,
    destination] or [file pattern]
    :return: list input_file_pattern_paths: input_file_pattern modified to
    include container name
    """
    # Initialise lists to store modified pattern lists and errors
    input_file_pattern_paths = []
    errors = []
    for input_pattern in input_file_pattern:
        # If the length of the pattern is one, the output folder has not
        # been specified
        if len(input_pattern) == 1:
            # Add the container name to the destination, as the working
            # directory will be named after the container
            input_pattern.append('')
        # If the length is two, the destination folder has been provided
        elif len(input_pattern) == 2:
            # Update the destination folder with the container name
            input_pattern[1] = os.path.join(input_pattern[1], '')
        # A different length indicates that the argument was
        # provided incorrectly
        else:
            errors.append(' ' .join(input_pattern))
            continue
        # Add the pattern plus updated destination to the list
        input_file_pattern_paths.append(input_pattern)
    # If there were errors with the supplied patterns, tell the user, and quit
    if errors:
        logging.error(
            'The following file input pattern(s) are not formatted '
            'correctly: %s', ', '.join(errors)
        )
        raise SystemExit
    return input_file_pattern_paths


def prep_resource_files(
        input_file_pattern: list,
        blob_service_client: BlobServiceClient,
        resource_file_list: str):
    """
    Parse the supplied resource file patterns, and write all the files
    matched by those pattern to a local file
    :param list input_file_pattern:
    :param blob_service_client: BlobServiceClient
    :param str resource_file_list: Name and path of file to which the matches
        to the provided expressions are to be written
    """
    # Create a list to store any patterns that did not return any files
    missing_patterns = []
    # Iterate over all the input patterns
    for pattern_destination in input_file_pattern:
        # The pattern_destination consists of a list of [file pattern, file
        # destination]. We only want the pattern
        container_expression = pattern_destination[0]
        # The container_expression consists of a path with
        # container_name/expression. Extract the container name
        container_name = str(Path(container_expression).parts[0])
        # Extract the path of the file from the container name
        expression = str(
            Path(container_expression).relative_to(container_name)
        )
        # If a folder was supplied, add an asterisk to target all files
        # in that folder
        if expression.endswith('/'):
            expression += '*'
        # Create a container client
        container_client = create_container_client(
            blob_service_client=blob_service_client,
            container_name=container_name,
            create=False
        )
        # Calculate size of output file before listing files for the
        # current pattern
        try:
            file_size = os.path.getsize(resource_file_list)
        except FileNotFoundError:
            file_size = 0
        # Suppress the print statements from AzureList
        sys.stdout = open(os.devnull, 'w', encoding='utf-8')
        # Write all files matching the expression to a local file
        try:
            AzureList.list_files(
                container_client=container_client,
                expression=expression,
                output_file=resource_file_list,
                container_name=container_name
            )
        except ResourceNotFoundError:
            missing_patterns.append(
                [container_name, 'container does not exist']
            )
            continue
        # Allow normal printing again
        sys.stdout = sys.__stdout__
        # Compare the size of the output file to its size before
        # AzureList searched for files
        try:
            updated_file_size = os.path.getsize(resource_file_list)
        except FileNotFoundError:
            updated_file_size = 0
        if file_size == updated_file_size:
            missing_patterns.append([container_name, container_expression])
    # Raise an error if one or more of the file matching patterns returned
    # no files
    if missing_patterns:
        logging.error(
            'Could not locate files for the following container: expression '
            'combination(s)'
        )
        for missing_pattern in missing_patterns:
            logging.error('\t%s: %s', missing_pattern[0], missing_pattern[1])
        raise SystemExit


def parse_resource_file_list(resource_file_list: str) -> list:
    """
    Parse the resource_file_list file, and create a list of all the files
    :param str resource_file_list: Name and path of file in which the matches
    to the provided expressions are written
    :return: list resource_files: All files extracted from resource_file_list
    """
    # Create a list to store all the resource files
    resource_files = []
    # Open the file, and read in the lines to a list
    with open(
            resource_file_list,
            'r',
            encoding='utf-8') as resource_file_output:
        for line in resource_file_output:
            # Split the line on tabs
            split_line = line.rstrip().split('\t')
            resource_files.append(split_line)
    return resource_files


def sas_url_prep(
        settings: Settings,
        container_name: str,
        blob_name: str) -> batchmodels.ResourceFile:
    """
    Create a SAS token and corresponding SAS URL for a blob in a container
    in Azure storage
    :param Settings settings: Class containing environment variable
    :param str container_name: The name of the Azure Blob storage container.
    :param str blob_name: The name of the file in blob storage
    :return batchmodels.ResourceFile: Initialised with a SAS URL
    """
    # Create the SAS token for the container:blob combination
    sas_token = generate_blob_sas(
        account_name=settings.azure_account_name,
        container_name=container_name,
        blob_name=blob_name,
        account_key=settings.azure_account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=24)
    )
    sas_url = generate_sas_url(
        account_name=settings.azure_account_name,
        account_domain='blob.core.windows.net',
        container_name=container_name,
        blob_name=blob_name,
        sas_token=sas_token
    )
    return batchmodels.ResourceFile(
            http_url=sas_url,
            file_path=blob_name
        )


def match_file_and_expression(
        resource_files: list,
        input_file_pattern_paths: list,
        container: str) -> list:
    """
    Match resource files to input file patterns in order to prepare the
    AzureAutomate batch document that requires
    the destination folder
    :param list resource_files: Files matched by AzureList
    :param list input_file_pattern_paths: List of lists of [file pattern,
    destination]
    :param str container: Name of container in Azure storage into which files
    are to be copied
    :return: list resource_files_with_input: List of lists of [container_name,
    resource_file_name, destination]
    """
    # Initialise the list to store the outputs
    resource_files_with_output = []
    # Iterate over all the input file patterns [container_name/expression,
    # destination]
    for container_destinations in input_file_pattern_paths:
        # Extract the container_name/expression field
        container_expression = container_destinations[0]
        # Split off the container name and path elements from the expression
        expression = os.path.basename(container_expression)
        # Replace any * with .* to be compatible with regex matching
        expression = expression.replace('*', '.*')
        # The destination folder is the second entry in the
        # container_destination list
        destination = container_destinations[1]
        # The container_expression consists of a path with
        # container_name/expression. Extract the container name
        container_name = str(Path(container_expression).parts[0])
        # Don't copy files already in the container
        if container_name == container:
            continue
        # If a folder has been specified, modify the nesting appropriately
        if container_expression.endswith('/'):
            # The folder must be included in the nesting if a folder
            # was specified
            nesting = str(
                Path(container_expression).relative_to(container_name)
            )
        else:
            # If an expression was provided, remove the folder from
            # the nesting variable
            nesting = os.path.dirname(
                str(Path(container_expression).relative_to(container_name))
            )

        # Iterate over all the files matched by AzureList
        for resource_file in resource_files:
            # Extract the container name and file name
            resource_container_name = resource_file[0]
            resource_file_name = resource_file[1]
            # Find the directory structure of the file
            resource_nesting = os.path.dirname(resource_file_name)
            # Determine if the directory structure of the input pattern
            # matches this file. If neither of the options have any nesting
            # i.e. are in the root of the blob/folder, they match
            if not nesting and not resource_nesting:
                nesting_match = True
            # Only if variables for both options exist
            elif nesting and resource_nesting:
                # If the resource file's folder structure is relative to the
                # pattern nesting folder, they are a match
                try:
                    nesting_match = Path(resource_nesting).relative_to(nesting)
                # A ValueError indicates that the paths are not relative
                # to each other
                except ValueError:
                    nesting_match = False
            # Otherwise the match is False
            else:
                nesting_match = False
            # Use regex to determine whether the file pattern matches
            # the file name
            expression_match = re.match(expression, resource_file_name)
            # In order for a file to match the pattern, the container names
            # must match, and both the folder structure
            # and the expressions must match
            if (container_name == resource_container_name
                    and nesting_match
                    and expression_match):
                # Add the container name, file name, and the destination
                # to the list
                resource_files_with_output.append(
                    [container_name, resource_file_name, destination]
                )
    return resource_files_with_output


def copy_blobs_to_container(
        blob_service_client,
        container_name,
        resource_files_with_output,
        settings):
    """
    Copies blobs from one Azure storage container to another.

    Parameters:
    container_name (str): The name of the destination container.
    resource_files_with_output (list): A list of tuples, where each tuple
    contains the source container name, the file name, and the
    destination path.
    settings (object): An object that contains Azure account settings,
    including the account name.

    The function iterates over the list of files to be copied. For each file,
    it creates an instance of the AzureMove class and calls its main method
    to perform the copy operation.
    """
    # Initialise a list to store any missing files
    missing = []

    for copy_operation in resource_files_with_output:

        # Rename the components of the list with useful variable names
        source_container = copy_operation[0]
        file_name = copy_operation[1]
        destination = copy_operation[2]

        # Log the copy information
        logging.warning(
            'Copying %s from %s to %s',
            file_name,
            source_container,
            container_name
        )

        # Copy the files to the appropriate container
        copy_file = AzureMove(
            object_name=file_name,
            container_name=source_container,
            account_name=settings.azure_account_name,
            target_container=container_name,
            path=destination,
            storage_tier='Hot',
            category='file',
            copy=True,
            name=None
        )
        copy_file.main()

        # Check to see if the blob copied successfully
        if not check_blob_exists(
            blob_name=file_name,
            blob_service_client=blob_service_client,
            container_name=container_name
        ):
            # Add the file to the list of missing files
            missing.append(
                f'{file_name} from {source_container} to {container_name}'
            )

        # Log the missing files
        if missing:
            logging.error(
                'The following files did not successfully copy: %s ',
                ';'.join(missing)
            )
            raise SystemExit

        logging.info(
            'The file copy operation complete successfully'
        )


def check_blob_exists(blob_name, blob_service_client, container_name):
    """
    Checks if a blob exists in the specified container.

    Parameters:
    blob_service_client: BlobServiceClient
    blob_name (str): The name of the blob to check.
    container_name (str): The name of the container.

    Returns:
    bool: True if the blob exists, False otherwise.
    """
    try:
        # Get a client for the target container
        container_client = blob_service_client.get_container_client(
            container_name
        )

        # Try to get the blob's properties
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.get_blob_properties()

        # If the above line did not raise an exception, the blob exists
        return True
    except ResourceNotFoundError:
        # If the blob is not found, a ResourceNotFoundError will be raised
        return False


def read_command_file(command_file: str) -> list:
    """
    Read in the supplied command file, and return a string
    :param str command_file: Name and path of file containing system call to
    perform on the nodes
    :return: list sys_call: List of all system calls to perform on the nodes
    """
    # Ensure that the file exists
    if not os.path.isfile(command_file):
        logging.error(
            "Could not locate supplied file containing system call to use: %s",
            command_file
        )
        raise SystemExit
    sys_call = []
    # Open the file, read in the commands, and add them to a list
    with open(command_file, 'r', encoding='utf-8') as command:
        for line in command:
            sys_call.append(line.rstrip())
    return sys_call


def download_files(
        container_name: str,
        download_file_pattern: list,
        path: str,
        settings: Settings
):
    """
    Use AzureDownload to download file(s)/folder(s) created as outputs
    from tasks
    :param str container_name: The name of the Azure Blob storage container.
    :param list download_file_pattern: List of lists of file(s)/folder(s)
    to download
    :param str path: Name and path of folder into which the file(s)/folder(s)
    are to be downloaded
    :param Settings settings: Class containing environment variables
    """
    # Iterate over all the requested file(s)/folder(s)
    for download_pattern in download_file_pattern:
        # Check if a folder was specified
        if download_pattern[0].endswith('/'):
            # Use AzureDownload folder to download folders
            download = AzureDownload(
                object_name=download_pattern[0],
                container_name=container_name,
                output_path=path,
                account_name=settings.azure_account_name,
                category='folder'
            )
        # Use AzureDownload file to download files
        else:
            download = AzureDownload(
                object_name=download_pattern[0],
                container_name=container_name,
                output_path=path,
                account_name=settings.azure_account_name,
                category='file'
            )
        # Download the file/folder
        download.main()
