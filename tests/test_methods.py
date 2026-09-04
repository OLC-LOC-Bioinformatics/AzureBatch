"""
Collection of tests for methods.py
"""

# Standard imports
import os
import shlex
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, PropertyMock

# Third party imports
import azure.batch.models as batchmodels
from azure.batch.models import BatchErrorException
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# Local imports
from azure_batch.methods import (
    Settings,
    TqdmUpTo,
    add_tasks,
    create_job,
    create_pool,
    generate_sas_url,
    prep_output_container,
    print_batch_exception,
    upload_file_to_container,
)


def load_env_variables():
    """
    Load environment variables from an env file if it exists. Create a
    dictionary of the environment variable name: its value.
    """
    dotenv_path = Path("env")
    if dotenv_path.exists():
        load_dotenv(dotenv_path=dotenv_path)

    return {
        "AZURE_ACCOUNT_NAME": os.getenv("AZURE_ACCOUNT_NAME"),
        "AZURE_ACCOUNT_KEY": os.getenv("AZURE_ACCOUNT_KEY"),
        "BATCH_ACCOUNT_URL": os.getenv("BATCH_ACCOUNT_URL"),
        "VM_SECRET": os.getenv("VM_SECRET"),
        "VM_TENANT": os.getenv("VM_TENANT"),
    }


def settings_values():
    """Return a complete non-secret settings mapping for unit tests."""
    return {
        "AZURE_ACCOUNT_NAME": "storage",
        "AZURE_ACCOUNT_KEY": "storage-key",
        "BATCH_ACCOUNT_URL": "https://batch.example.test",
        "BATCH_ACCOUNT_SUBNET": "/subscriptions/test/subnets/batch",
        "BATCH_SECURITY_TYPE": "trustedLaunch",
        "VM_SECRET": "secret",
        "VM_CLIENT_ID": "client-id",
        "VM_TENANT": "tenant-id",
        "VM_IMAGE": "/images/cowbat/versions/1",
        "COWBAT_NODE_AGENT_SKU": "batch.node.ubuntu 22.04",
        "AMPLISEQ_IMAGE": "/images/ampliseq/versions/1",
        "AMPLISEQ_NODE_AGENT_SKU": "batch.node.ubuntu 20.04",
        "COWSNPHR_IMAGE": "/images/cowsnphr/versions/1",
        "COWSNPHR_NODE_AGENT_SKU": "batch.node.ubuntu 22.04",
        "NANOPORE_IMAGE": "/images/nanopore/versions/0.0.1",
        "NANOPORE_NODE_AGENT_SKU": "batch.node.ubuntu 24.04",
        "NANOPORE_BATCH_VM_SIZE": "Standard_NV18ads_A10_v5",
        "NANOPORE_SECURITY_TYPE": "trustedLaunch",
        "NANOPORE_SECURE_BOOT_ENABLED": "false",
        "NANOPORE_VTPM_ENABLED": "false",
    }


def create_blob_service_client(env_vars) -> BlobServiceClient:
    """Create a BlobServiceClient using the supplied environment mapping."""
    return BlobServiceClient(
        account_url=(
            f"https://{env_vars['AZURE_ACCOUNT_NAME']}.blob.core.windows.net/"
        ),
        credential=env_vars["AZURE_ACCOUNT_KEY"],
    )


class TestTqdmUpTo:
    """Tests for TqdmUpTo."""

    def test_update_to(self):
        progress = TqdmUpTo(total=100)
        response = Mock()
        response.context = {
            "upload_stream_current": 50,
            "data_stream_total": 100,
        }
        progress.update_to(response)
        assert progress.total == 100
        assert progress.n == 50

        response.context = {
            "upload_stream_current": 75,
            "data_stream_total": 100,
        }
        progress.update_to(response)
        assert progress.total == 100
        assert progress.n == 75


class TestSettings:
    """Tests for Settings."""

    def test_init(self):
        settings = {
            "AZURE_ACCOUNT_NAME": "test_azure_account_name",
            "AZURE_ACCOUNT_KEY": "test_azure_account_key",
            "BATCH_ACCOUNT_URL": "test_batch_account_url",
            "BATCH_ACCOUNT_SUBNET": "test_batch_account_subnet",
            "VM_SECRET": "test_vm_secret",
            "VM_CLIENT_ID": "test_vm_client_id",
            "VM_TENANT": "test_vm_tenant",
            "VM_IMAGE": "test_vm_image",
            "COWBAT_NODE_AGENT_SKU": "test_node_agent_sku",
        }

        settings_obj = Settings(settings=settings, analysis_type="COWBAT")

        assert settings_obj.azure_account_name == "test_azure_account_name"
        assert settings_obj.azure_account_key == "test_azure_account_key"
        assert settings_obj.batch_account_url == "test_batch_account_url"
        assert settings_obj.vm_secret == "test_vm_secret"
        assert settings_obj.vm_tenant == "test_vm_tenant"
        assert settings_obj.security_type == "trustedLaunch"
        assert settings_obj.secure_boot_enabled is None
        assert settings_obj.v_tpm_enabled is None
        assert settings_obj.vm_size is None


class TestPrintBatchException:
    """Tests for print_batch_exception."""

    @patch("builtins.print")
    def test_print_batch_exception(self, mock_print):
        error = Mock(spec=BatchErrorException)
        type(error).message = PropertyMock(return_value="test_message")
        error.error = Mock()
        error.error.message = Mock()
        error.error.message.value = "test_value"
        error.error.values = [
            Mock(key="test_key1", value="test_value1"),
            Mock(key="test_key2", value="test_value2"),
        ]

        print_batch_exception(error)

        mock_print.assert_any_call("-------------------------------------------")
        mock_print.assert_any_call("Exception encountered:")
        mock_print.assert_any_call("test_value")
        mock_print.assert_any_call()
        mock_print.assert_any_call("test_key1:\ttest_value1")
        mock_print.assert_any_call("test_key2:\ttest_value2")
        mock_print.assert_any_call("-------------------------------------------")


class TestUploadFileToContainer:
    """Tests for upload_file_to_container."""

    @patch("azure.storage.blob.BlobServiceClient.get_blob_client")
    @patch("azure_batch.methods.logger.warning")
    def test_upload_file_to_container(self, mock_warning, mock_get_blob_client):
        env_vars = load_env_variables()
        blob_service_client = create_blob_service_client(env_vars=env_vars)
        blob_client = Mock()
        mock_get_blob_client.return_value = blob_client

        with tempfile.TemporaryDirectory() as tmp_dir:
            file_path = os.path.join(tmp_dir, "test.txt")
            with open(file_path, "w", encoding="utf-8") as tmp_file:
                tmp_file.write("test content")

            upload_file_to_container(
                blob_service_client,
                "test_container",
                file_path,
                tmp_dir,
            )

            blob_client.upload_blob.assert_called_once()
            mock_warning.assert_called_once_with(
                "File '%s' successfully uploaded.",
                "test.txt",
            )

        blob_client.delete_blob()

    @patch("azure.storage.blob.BlobServiceClient.get_blob_client")
    @patch("azure_batch.methods.logger.warning")
    def test_upload_file_to_container_exists(
        self,
        mock_warning,
        mock_get_blob_client,
    ):
        env_vars = load_env_variables()
        blob_service_client = create_blob_service_client(env_vars=env_vars)
        blob_client = Mock()
        blob_client.upload_blob.side_effect = ResourceExistsError()
        mock_get_blob_client.return_value = blob_client

        with tempfile.TemporaryDirectory() as tmp_dir:
            file_path = os.path.join(tmp_dir, "test.txt")
            with open(file_path, "w", encoding="utf-8") as tmp_file:
                tmp_file.write("test content")

            upload_file_to_container(
                blob_service_client,
                "test_container",
                file_path,
                tmp_dir,
            )

            mock_warning.assert_called_once_with(
                "File '%s' already exists. Skipping...",
                "test.txt",
            )

        blob_client.delete_blob()


def test_generate_sas_url():
    env_vars = load_env_variables()
    sas_token = "test_sas_token"

    sas_url = generate_sas_url(
        account_name=env_vars["AZURE_ACCOUNT_NAME"],
        account_domain="blob.core.windows.net",
        container_name="test_container",
        blob_name="test.txt",
        sas_token=sas_token,
    )
    assert sas_url == (
        f"https://{env_vars['AZURE_ACCOUNT_NAME']}.blob.core.windows.net/"
        f"test_container/test.txt?{sas_token}"
    )

    sas_url = generate_sas_url(
        account_name=env_vars["AZURE_ACCOUNT_NAME"],
        account_domain="blob.core.windows.net",
        container_name="test_container",
        blob_name=None,
        sas_token=sas_token,
    )
    assert sas_url == (
        f"https://{env_vars['AZURE_ACCOUNT_NAME']}.blob.core.windows.net/"
        f"test_container?{sas_token}"
    )


def test_create_pool():
    env_vars = load_env_variables()
    batch_service_client = Mock()
    settings = Mock()
    settings.vm_image = "test_vm_image"
    settings.azure_account_name = env_vars["AZURE_ACCOUNT_NAME"]
    settings.azure_account_key = "test_account_key"
    settings.node_agent_sku_id = "batch.node.ubuntu 20.04"
    settings.batch_account_subnet = "test_subnet"
    settings.security_type = "trustedLaunch"
    settings.secure_boot_enabled = None
    settings.v_tpm_enabled = None
    settings.vm_size = None

    create_pool(
        batch_service_client=batch_service_client,
        pool_id="test_pool_id",
        vm_size="test_vm_size",
        container_name="test_container_name",
        mount_path="test_mount_path",
        settings=settings,
    )

    actual_call = batch_service_client.pool.add.call_args[0][0]
    assert actual_call.id == "test_pool_id"
    assert (
        actual_call.virtual_machine_configuration.image_reference.
        virtual_machine_image_id
        == "test_vm_image"
    )
    assert (
        actual_call.virtual_machine_configuration.node_agent_sku_id
        == "batch.node.ubuntu 20.04"
    )
    security_profile = actual_call.virtual_machine_configuration.security_profile
    assert security_profile is not None
    assert security_profile.security_type == "trustedLaunch"
    assert security_profile.uefi_settings is None
    assert actual_call.vm_size == "test_vm_size"
    assert actual_call.target_dedicated_nodes == 1
    assert actual_call.task_slots_per_node == 1

    blob_cfg = (
        actual_call.mount_configuration[0].azure_blob_file_system_configuration
    )
    assert blob_cfg.account_name == env_vars["AZURE_ACCOUNT_NAME"]
    assert blob_cfg.account_key == "test_account_key"
    assert blob_cfg.container_name == "test_container_name"
    assert blob_cfg.relative_mount_path == "test_mount_path"


def test_create_job():
    batch_service_client = Mock()
    create_job(
        batch_service_client=batch_service_client,
        job_id="test_job_id",
        pool_id="test_pool_id",
    )

    expected_job_add_parameter = batchmodels.JobAddParameter(
        id="test_job_id",
        pool_info=batchmodels.PoolInformation(pool_id="test_pool_id"),
    )
    batch_service_client.job.add.assert_called_once_with(
        expected_job_add_parameter
    )


def test_add_tasks():
    task_id = "test_task_id"
    tasks = []
    resource_input_files = ["input_file_1", "input_file_2"]
    resource_output_files = ["output_file_1", "output_file_2"]
    sys_call = "test_sys_call"

    result_tasks = add_tasks(
        task_id=task_id,
        tasks=tasks,
        resource_input_files=resource_input_files,
        resource_output_files=resource_output_files,
        sys_call=sys_call,
    )

    expected_task_add_parameter = batchmodels.TaskAddParameter(
        id=task_id,
        constraints=batchmodels.TaskConstraints(max_wall_clock_time="PT16H"),
        command_line=f"/bin/bash -c {shlex.quote(sys_call)}",
        resource_files=resource_input_files,
        output_files=resource_output_files,
        user_identity=batchmodels.UserIdentity(
            auto_user=batchmodels.AutoUserSpecification(
                elevation_level=batchmodels.ElevationLevel.admin,
                scope=batchmodels.AutoUserScope.pool,
            )
        ),
        environment_settings=[
            batchmodels.EnvironmentSetting(
                name="CONDA",
                value="/usr/bin/miniconda/bin",
            ),
        ],
    )

    assert len(result_tasks) == 1
    actual = result_tasks[0]
    assert actual.id == expected_task_add_parameter.id
    assert actual.command_line == expected_task_add_parameter.command_line
    assert actual.constraints.max_wall_clock_time == "PT16H"
    assert actual.resource_files == resource_input_files
    assert actual.output_files == resource_output_files
    assert (
        actual.user_identity.auto_user.elevation_level
        == batchmodels.ElevationLevel.admin
    )
    assert actual.user_identity.auto_user.scope == batchmodels.AutoUserScope.pool
    assert actual.environment_settings[0].name == "CONDA"
    assert actual.environment_settings[0].value == "/usr/bin/miniconda/bin"


@patch("azure_batch.methods.generate_container_sas", return_value="sas-token")
def test_prep_output_container(mock_generate_container_sas):
    """Test output-container creation and SAS URL construction."""
    blob_storage_service_client = Mock()
    settings = Mock(
        azure_account_name="storage",
        azure_account_key="storage-key",
    )
    output_container_name = "test-output-container"

    sas_url = prep_output_container(
        output_container_name=output_container_name,
        settings=settings,
        blob_storage_service_client=blob_storage_service_client,
    )

    blob_storage_service_client.create_container.assert_called_once_with(
        name=output_container_name
    )
    mock_generate_container_sas.assert_called_once()

    call_kwargs = mock_generate_container_sas.call_args.kwargs
    assert call_kwargs["account_name"] == "storage"
    assert call_kwargs["container_name"] == output_container_name
    assert call_kwargs["account_key"] == "storage-key"
    assert call_kwargs["permission"].read is True
    assert call_kwargs["permission"].write is True
    assert call_kwargs["expiry"] is not None

    assert sas_url == (
        "https://storage.blob.core.windows.net/"
        "test-output-container?sas-token"
    )


def test_all_analysis_types_use_trusted_launch():
    """Test that all analysis types use Trusted Launch."""
    values = settings_values()

    for analysis_type in (
        "COWBAT",
        "AmpliSeq",
        "COWSNPhR",
        "Nanopore",
    ):
        result = Settings(
            settings=values,
            analysis_type=analysis_type,
        )
        assert result.security_type == "trustedLaunch"


def test_general_uefi_settings_can_be_configured():
    """Test that general UEFI settings can be configured."""
    configured = settings_values()
    configured["BATCH_SECURE_BOOT_ENABLED"] = "true"
    configured["BATCH_VTPM_ENABLED"] = "true"

    result = Settings(
        settings=configured,
        analysis_type="COWBAT",
    )

    assert result.security_type == "trustedLaunch"
    assert result.secure_boot_enabled is True
    assert result.v_tpm_enabled is True
