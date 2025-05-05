"""
Collection of tests for methods.py
"""

# Standard imports
import datetime
from pathlib import Path
import os
import tempfile
from unittest.mock import (
    Mock,
    patch,
    PropertyMock
)

# Third party imports
from azure.core.exceptions import ResourceExistsError
import azure.batch.models as batchmodels
from azure.batch.models import BatchErrorException
from azure.storage.blob import (
    AccountSasPermissions,
    BlobServiceClient
)
from dotenv import load_dotenv

# Local imports
from azure_batch.methods import (
    add_tasks,
    create_job,
    create_pool,
    generate_container_sas,
    generate_sas_url,
    prep_output_container,
    print_batch_exception,
    Settings,
    TqdmUpTo,
    upload_file_to_container
)


def load_env_variables():
    """
    Load environment variables from an env file if it exists. Create a
    dictionary of the environment variable name: its value
    """
    # Load environment variables from .env file if it exists
    dotenv_path = Path('env')
    if dotenv_path.exists():
        load_dotenv(dotenv_path=dotenv_path)

    # Get the environment variables
    env_vars = {
        'AZURE_ACCOUNT_NAME': os.getenv('AZURE_ACCOUNT_NAME'),
        'AZURE_ACCOUNT_KEY': os.getenv('AZURE_ACCOUNT_KEY'),
        'BATCH_ACCOUNT_URL': os.getenv('BATCH_ACCOUNT_URL'),
        'VM_SECRET': os.getenv('VM_SECRET'),
        'VM_TENANT': os.getenv('VM_TENANT'),
    }
    return env_vars


def create_blob_service_client(env_vars) -> BlobServiceClient:
    """
    Create a blob service client using environment variables
    :param env_vars: type (dict): Dictionary of environment variable name:
        environment variable value
    """
    # Create and return the BlobServiceClient
    blob_service_client = BlobServiceClient(
        account_url=f'https://{env_vars['AZURE_ACCOUNT_NAME']}'
                    f'.blob.core.windows.net/',
        credential=env_vars['AZURE_ACCOUNT_KEY']
    )

    return blob_service_client


class TestTqdmUpTo:
    """
    Test class for the TqdmUpTo class.

    This class contains unit tests for the TqdmUpTo class, specifically the
    update_to method.
    """
    def test_update_to(self):
        """
        Test the update_to method of the TqdmUpTo class.

        This test creates a TqdmUpTo instance and a mock response object. It
        calls the update_to method with the mock response and checks that the
        total and current progress are updated as expected.
        """
        # Create an instance of TqdmUpTo
        progress = TqdmUpTo(total=100)

        # Mock the response object
        response = Mock()
        response.context = {
            'upload_stream_current': 50,
            'data_stream_total': 100
        }

        # Call the method to be tested
        progress.update_to(response)

        # Check that the total and current progress are as expected
        assert progress.total == 100
        assert progress.n == 50

        # Mock the response object with different values
        response.context = {
            'upload_stream_current': 75,
            'data_stream_total': 100
        }

        # Call the method to be tested
        progress.update_to(response)

        # Check that the total and current progress are as expected
        assert progress.total == 100
        assert progress.n == 75


class TestSettings:
    """
    Test class for the Settings class.

    This class contains unit tests for the Settings class, specifically the
    __init__ method.
    """

    def test_init(self):
        """
        Test the __init__ method of the Settings class.

        This test creates a Settings instance with a mock settings dictionary
        and checks that the attributes are set as expected.
        """
        # Mock the settings dictionary
        settings = {
            'AZURE_ACCOUNT_NAME': 'test_azure_account_name',
            'AZURE_ACCOUNT_KEY': 'test_azure_account_key',
            'BATCH_ACCOUNT_URL': 'test_batch_account_url',
            'BATCH_ACCOUNT_SUBNET': 'test_batch_account_subnet',
            'VM_SECRET': 'test_vm_secret',
            'VM_CLIENT_ID': 'test_vm_client_id',
            'VM_TENANT': 'test_vm_tenant',
            'VM_IMAGE': 'test_vm_image',
            'COWBAT_NODE_AGENT_SKU': 'test_node_agent_sku',
        }

        # Create an instance of Settings
        settings_obj = Settings(
            settings=settings,
            analysis_type='COWBAT'
        )

        # Check that the attributes are set as expected
        assert settings_obj.azure_account_name == 'test_azure_account_name'
        assert settings_obj.azure_account_key == 'test_azure_account_key'
        assert settings_obj.batch_account_url == 'test_batch_account_url'
        assert settings_obj.vm_secret == 'test_vm_secret'
        assert settings_obj.vm_tenant == 'test_vm_tenant'


class TestPrintBatchException:
    """
    Test class for the print_batch_exception function.

    This class contains unit tests for the print_batch_exception function.
    """

    @patch('builtins.print')
    def test_print_batch_exception(self, mock_print):
        """
        Test the print_batch_exception function.

        This test creates a BatchErrorException instance with a mock error
        and checks that the print function is called with the expected
        arguments.
        """
        # Mock the error
        error = Mock(spec=BatchErrorException)
        type(error).message = PropertyMock(return_value='test_message')
        error.error = Mock()
        error.error.message = Mock()
        error.error.message.value = 'test_value'
        error.error.values = [
            Mock(key='test_key1', value='test_value1'),
            Mock(key='test_key2', value='test_value2')
        ]

        # Call the function to be tested
        print_batch_exception(error)

        # Check that the print function is called with the expected arguments
        mock_print.assert_any_call(
            '-------------------------------------------'
        )
        mock_print.assert_any_call('Exception encountered:')
        mock_print.assert_any_call('test_value')
        mock_print.assert_any_call()
        mock_print.assert_any_call('test_key1:\ttest_value1')
        mock_print.assert_any_call('test_key2:\ttest_value2')
        mock_print.assert_any_call(
            '-------------------------------------------'
        )


class TestUploadFileToContainer:
    """
    Test class for the upload_file_to_container function.
    """

    @patch('azure.storage.blob.BlobServiceClient.get_blob_client')
    @patch('logging.warning')
    def test_upload_file_to_container(
        self, mock_warning, mock_get_blob_client
    ):
        """
        Test the upload_file_to_container function.
        """
        # Load the environment variables
        env_vars = load_env_variables()
        # Create the BlobServiceClient
        blob_service_client = create_blob_service_client(env_vars=env_vars)

        # Mock the blob client
        blob_client = Mock()
        mock_get_blob_client.return_value = blob_client

        # Create temporary directory and file
        with tempfile.TemporaryDirectory() as tmp_dir:
            file_path = os.path.join(tmp_dir, 'test.txt')
            with open(file_path, 'w', encoding='utf-8') as tmp_file:
                tmp_file.write('test content')

            # Call the function to be tested
            upload_file_to_container(
                blob_service_client, 'test_container', file_path, tmp_dir
            )

            # Check that the upload_blob method is called
            blob_client.upload_blob.assert_called_once()

            # Check that the warning method is called
            mock_warning.assert_called_once_with(
                "File '%s' successfully uploaded.", 'test.txt'
            )

        # Clean-up step: remove the file from blob storage
        blob_client.delete_blob()

    @patch('azure.storage.blob.BlobServiceClient.get_blob_client')
    @patch('logging.warning')
    def test_upload_file_to_container_exists(
        self, mock_warning, mock_get_blob_client
    ):
        """
        Test the upload_file_to_container function when the file exists.
        """
        # Load the environment variables
        env_vars = load_env_variables()
        # Create the BlobServiceClient
        blob_service_client = create_blob_service_client(env_vars=env_vars)

        # Mock the blob client
        blob_client = Mock()
        blob_client.upload_blob.side_effect = ResourceExistsError()
        mock_get_blob_client.return_value = blob_client

        # Create temporary directory and file
        with tempfile.TemporaryDirectory() as tmp_dir:
            file_path = os.path.join(tmp_dir, 'test.txt')
            with open(file_path, 'w', encoding='utf-8') as tmp_file:
                tmp_file.write('test content')

            # Call the function to be tested
            upload_file_to_container(
                blob_service_client, 'test_container', file_path, tmp_dir
            )

            # Check that the warning method is called
            mock_warning.assert_called_once_with(
                "File '%s' already exists. Skipping...", 'test.txt'
            )
        # Clean-up step: remove the file from blob storage
        blob_client.delete_blob()


def test_generate_sas_url():
    """
    Generate SAS URL for a blob in a container
    """
    # Load the environment variables
    env_vars = load_env_variables()

    # Create a SAS token for testing
    # Note: In a real test, you would generate a real SAS token
    sas_token = 'test_sas_token'

    # Call the function to be tested
    sas_url = generate_sas_url(
        account_name=env_vars['AZURE_ACCOUNT_NAME'],
        account_domain='blob.core.windows.net',
        container_name='test_container',
        blob_name='test.txt',
        sas_token=sas_token
    )

    # Check the SAS URL
    assert sas_url == (
        f'https://{env_vars["AZURE_ACCOUNT_NAME"]}.blob.core.windows.net/'
        f'test_container/test.txt?{sas_token}'
    )

    # Call the function to be tested without a blob name
    sas_url = generate_sas_url(
        account_name=env_vars['AZURE_ACCOUNT_NAME'],
        account_domain='blob.core.windows.net',
        container_name='test_container',
        blob_name=None,
        sas_token=sas_token
    )

    # Check the SAS URL
    assert sas_url == (
        f'https://{env_vars["AZURE_ACCOUNT_NAME"]}.blob.core.windows.net/'
        f'test_container?{sas_token}'
    )


def test_create_pool():
    """
    Test the create_pool function to ensure it's calling the pool.add method
    of the BatchServiceClient with the correct arguments.
    """
    env_vars = load_env_variables()
    batch_service_client = Mock()
    settings = Mock()
    settings.vm_image = 'test_vm_image'
    settings.azure_account_name = env_vars['AZURE_ACCOUNT_NAME']
    settings.azure_account_key = 'test_account_key'
    settings.node_agent_sku_id = "batch.node.ubuntu 20.04"
    settings.batch_account_subnet = "test_subnet"

    create_pool(
        batch_service_client=batch_service_client,
        pool_id='test_pool_id',
        vm_size='test_vm_size',
        container_name='test_container_name',
        mount_path='test_mount_path',
        settings=settings
    )

    # Get the actual PoolAddParameter passed to pool.add
    actual_call = batch_service_client.pool.add.call_args[0][0]

    # Assert key fields
    assert actual_call.id == 'test_pool_id'
    assert (
        actual_call.virtual_machine_configuration.image_reference.
        virtual_machine_image_id == 'test_vm_image'
    )
    assert actual_call.virtual_machine_configuration.node_agent_sku_id == \
        "batch.node.ubuntu 20.04"
    assert actual_call.vm_size == 'test_vm_size'
    assert actual_call.target_dedicated_nodes == 1

    blob_cfg = \
        actual_call.mount_configuration[0].azure_blob_file_system_configuration
    assert blob_cfg.account_name == env_vars['AZURE_ACCOUNT_NAME']
    assert blob_cfg.account_key == 'test_account_key'
    assert blob_cfg.container_name == 'test_container_name'
    assert blob_cfg.relative_mount_path == 'test_mount_path'


def test_create_job():
    """
    Test the create_job function to ensure it's calling the job.add method
    of the BatchServiceClient with the correct arguments.
    """
    # Create a mock BatchServiceClient
    batch_service_client = Mock()

    # Call the function to be tested
    create_job(
        batch_service_client=batch_service_client,
        job_id='test_job_id',
        pool_id='test_pool_id'
    )

    # Define the expected JobAddParameter
    expected_job_add_parameter = batchmodels.JobAddParameter(
        id='test_job_id',
        pool_info=batchmodels.PoolInformation(pool_id='test_pool_id')
    )

    # Check that the job.add method was called with the correct arguments
    batch_service_client.job.add.assert_called_once_with(
        expected_job_add_parameter
    )


def test_add_tasks():
    """
    Test the add_tasks function to ensure it's appending the correct task
    to the tasks list.
    """
    # Define the input parameters
    task_id = 'test_task_id'
    tasks = []
    resource_input_files = ['input_file_1', 'input_file_2']
    resource_output_files = ['output_file_1', 'output_file_2']
    sys_call = 'test_sys_call'

    # Call the function to be tested
    result_tasks = add_tasks(
        task_id=task_id,
        tasks=tasks,
        resource_input_files=resource_input_files,
        resource_output_files=resource_output_files,
        sys_call=sys_call
    )

    # Define the expected TaskAddParameter
    expected_task_add_parameter = batchmodels.TaskAddParameter(
        id=task_id,
        constraints=batchmodels.TaskConstraints(max_wall_clock_time="PT16H"),
        command_line=f'/bin/bash -c \"{sys_call}\"',
        resource_files=resource_input_files,
        output_files=resource_output_files,
        user_identity=batchmodels.UserIdentity(
            auto_user=batchmodels.AutoUserSpecification(
                elevation_level=batchmodels.ElevationLevel.admin,
                scope=batchmodels.AutoUserScope.pool
            )
        ),
        environment_settings=[
            batchmodels.EnvironmentSetting(
                name='CONDA',
                value='/usr/bin/miniconda/bin'
            ),
        ]
    )

    # Check that the correct task was appended to the tasks list
    assert len(result_tasks) == 1
    assert result_tasks[0] == expected_task_add_parameter


def test_prep_output_container():
    """
    Test the prep_output_container function to ensure it's creating the
    container and returning the correct SAS URL.
    """
    # Create a mock BlobServiceClient
    blob_storage_service_client = Mock()

    # Load the environment variables
    env_vars = load_env_variables()
    # Create a mock Settings object
    settings = Mock()
    settings.azure_account_name = env_vars['AZURE_ACCOUNT_NAME']
    settings.azure_account_key = env_vars['AZURE_ACCOUNT_KEY']

    # Define the output container name
    output_container_name = 'test_output_container_name'

    # Call the function to be tested
    sas_url = prep_output_container(
        output_container_name=output_container_name,
        settings=settings,
        blob_storage_service_client=blob_storage_service_client
    )

    # Check that the create_container method was called with the correct
    # arguments
    blob_storage_service_client.create_container.assert_called_once_with(
        name=output_container_name
    )

    # Define the expected SAS URL
    expected_sas_url = generate_sas_url(
        account_name=settings.azure_account_name,
        account_domain='blob.core.windows.net',
        container_name=output_container_name,
        blob_name=str(),
        sas_token=generate_container_sas(
            account_name=settings.azure_account_name,
            container_name=output_container_name,
            account_key=settings.azure_account_key,
            permission=AccountSasPermissions(read=True, write=True),
            expiry=datetime.datetime.now(datetime.timezone.utc) +
            datetime.timedelta(hours=24)
        )
    )

    # Check that the correct SAS URL was returned
    assert sas_url == expected_sas_url
