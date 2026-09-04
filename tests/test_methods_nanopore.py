"""Focused unit tests for Azure Batch helper methods and Nanopore support."""

import datetime
import io
import shlex
from pathlib import Path
from unittest.mock import Mock, patch

import azure.batch.models as batchmodels
import pytest
from azure.core.exceptions import ResourceExistsError
from azure_batch.methods import (
    Settings,
    TqdmUpTo,
    add_tasks,
    create_pool,
    generate_sas_url,
    log_output_resource_files,
    parse_boolean_setting,
    parse_resource_input_pattern,
    prep_output_container,
    read_command_file,
    wait_for_tasks_to_complete,
)


def settings_values():
    """Return a complete, non-secret settings mapping for unit tests."""
    return {
        "AZURE_ACCOUNT_NAME": "storage",
        "AZURE_ACCOUNT_KEY": "storage-key",
        "BATCH_ACCOUNT_URL": "https://batch.example.test",
        "BATCH_ACCOUNT_SUBNET": "/subscriptions/test/subnets/batch",
        "BATCH_VM_SIZE": "Standard_D32s_v3",
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
        "NANOPORE_CONDA_PATH": "/opt/micromamba/bin",
        "NANOPORE_MAMBA_ROOT_PREFIX": "/opt/micromamba/root",
        "NANOPORE_RUNTIME_BIN_PATH": (
            "/opt/micromamba/root/envs/poresippr/bin:"
            "/opt/micromamba/bin:"
            "/opt/ont/dorado/bin:"
            "/usr/local/bin:"
            "/usr/bin:"
            "/bin"
        ),
    }


class TestTqdmUpTo:
    def test_init_preserves_tqdm_total(self):
        progress = TqdmUpTo(total=100, disable=False, file=io.StringIO())
        try:
            assert progress.total == 100
        finally:
            progress.close()

    def test_update_to_uses_absolute_transfer_position(self):
        progress = TqdmUpTo(total=100, disable=False, file=io.StringIO())
        response = Mock()
        try:
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
            assert progress.n == 75
        finally:
            progress.close()

    def test_update_to_ignores_missing_progress_values(self):
        progress = TqdmUpTo(total=None, disable=False, file=io.StringIO())
        response = Mock()
        response.context = {
            "upload_stream_current": None,
            "data_stream_total": None,
        }
        try:
            assert progress.update_to(response) is None
            assert progress.n == 0
        finally:
            progress.close()


@pytest.mark.parametrize(
    ("analysis_type", "image_key", "sku_key"),
    [
        ("COWBAT", "VM_IMAGE", "COWBAT_NODE_AGENT_SKU"),
        ("AmpliSeq", "AMPLISEQ_IMAGE", "AMPLISEQ_NODE_AGENT_SKU"),
        ("COWSNPhR", "COWSNPHR_IMAGE", "COWSNPHR_NODE_AGENT_SKU"),
        ("Nanopore", "NANOPORE_IMAGE", "NANOPORE_NODE_AGENT_SKU"),
    ],
)
def test_settings_selects_analysis_image(analysis_type, image_key, sku_key):
    values = settings_values()
    result = Settings(values, analysis_type)
    assert result.vm_image == values[image_key]
    assert result.node_agent_sku_id == values[sku_key]
    assert result.vm_client_id == values["VM_CLIENT_ID"]


def test_nanopore_settings_default_to_trusted_launch():
    values = settings_values()
    result = Settings(values, "Nanopore")

    assert result.security_type == "trustedLaunch"
    assert result.vm_size == "Standard_NV18ads_A10_v5"


def test_nanopore_settings_honor_explicit_security_type():
    values = settings_values()
    values["NANOPORE_SECURITY_TYPE"] = "confidentialvm"
    result = Settings(values, "Nanopore")

    assert result.security_type == "confidentialvm"


@pytest.mark.parametrize(
    "analysis_type",
    [
        "COWBAT",
        "AmpliSeq",
        "COWSNPhR",
    ],
)
def test_non_nanopore_settings_use_default_trusted_launch(
    analysis_type,
):
    result = Settings(
        settings_values(),
        analysis_type,
    )

    assert result.security_type == "trustedLaunch"
    assert result.vm_size == "Standard_D32s_v3"
    assert result.secure_boot_enabled is None
    assert result.v_tpm_enabled is None
    assert result.conda_path == "/usr/bin/miniconda/bin"
    assert result.runtime_bin_path == "/usr/bin/miniconda/bin"
    assert result.mamba_root_prefix is None


def test_settings_rejects_unsupported_analysis_type():
    with pytest.raises(ValueError, match="Unsupported analysis type"):
        Settings(settings_values(), "Unknown")


def test_create_pool_applies_nanopore_security_and_vm_defaults():
    values = settings_values()
    settings = Settings(values, "Nanopore")
    client = Mock()

    create_pool(
        batch_service_client=client,
        pool_id="nanopore-pool",
        vm_size=None,
        container_name="nanopore-runs",
        mount_path="nanopore-runs",
        settings=settings,
    )

    client.pool.add.assert_called_once()
    pool = client.pool.add.call_args.args[0]
    vm_config = pool.virtual_machine_configuration

    assert pool.id == "nanopore-pool"
    assert pool.vm_size == "Standard_NV18ads_A10_v5"
    assert pool.target_dedicated_nodes == 1
    assert pool.task_slots_per_node == 1
    assert pool.network_configuration.subnet_id == values["BATCH_ACCOUNT_SUBNET"]
    assert (
        pool.network_configuration.public_ip_address_configuration.provision
        == "noPublicIPAddresses"
    )
    assert vm_config.image_reference.virtual_machine_image_id == values[
        "NANOPORE_IMAGE"
    ]
    assert vm_config.node_agent_sku_id == values["NANOPORE_NODE_AGENT_SKU"]
    assert vm_config.security_profile.security_type == "trustedLaunch"
    assert vm_config.security_profile.uefi_settings.secure_boot_enabled is False
    assert vm_config.security_profile.uefi_settings.v_tpm_enabled is False

    assert len(pool.mount_configuration) == 1
    blob_mount = pool.mount_configuration[0].azure_blob_file_system_configuration
    assert blob_mount.account_name == values["AZURE_ACCOUNT_NAME"]
    assert blob_mount.container_name == "nanopore-runs"
    assert blob_mount.relative_mount_path == "nanopore-runs"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (True, True),
        (False, False),
        ("true", True),
        ("false", False),
        ("TRUE", True),
        ("FALSE", False),
        ("yes", True),
        ("no", False),
        ("1", True),
        ("0", False),
        ("on", True),
        ("off", False),
    ],
)
def test_parse_boolean_setting(value, expected):
    assert parse_boolean_setting(value) is expected


def test_nanopore_settings_disable_secure_boot_and_vtpm():
    result = Settings(settings_values(), "Nanopore")

    assert result.security_type == "trustedLaunch"
    assert result.secure_boot_enabled is False
    assert result.v_tpm_enabled is False


def test_nanopore_settings_honor_explicit_uefi_values():
    values = settings_values()
    values["NANOPORE_SECURE_BOOT_ENABLED"] = "true"
    values["NANOPORE_VTPM_ENABLED"] = "yes"

    result = Settings(values, "Nanopore")

    assert result.secure_boot_enabled is True
    assert result.v_tpm_enabled is True


@pytest.mark.parametrize(
    ("default", "expected"),
    [
        (True, True),
        (False, False),
        (None, None),
    ],
)
def test_parse_boolean_setting_uses_default(default, expected):
    assert parse_boolean_setting(None, default=default) is expected


def test_parse_boolean_setting_rejects_invalid_value():
    with pytest.raises(ValueError, match="Unsupported boolean setting"):
        parse_boolean_setting("maybe")


def test_create_pool_prefers_explicit_vm_size():
    settings = Settings(settings_values(), "Nanopore")
    client = Mock()

    create_pool(
        batch_service_client=client,
        pool_id="nanopore-pool",
        vm_size="Standard_D4ds_v5",
        container_name="nanopore-runs",
        mount_path="nanopore-runs",
        settings=settings,
    )

    pool = client.pool.add.call_args.args[0]
    assert pool.vm_size == "Standard_D4ds_v5"


@pytest.mark.parametrize(
    ("analysis_type", "vm_size"),
    [
        ("COWBAT", "Standard_D32s_v3"),
        ("AmpliSeq", "Standard_D16s_v5"),
        ("COWSNPhR", "Standard_D32s_v5"),
    ],
)
def test_create_pool_applies_trusted_launch_to_existing_workflows(
    analysis_type,
    vm_size,
):
    settings = Settings(settings_values(), analysis_type)
    client = Mock()

    create_pool(
        batch_service_client=client,
        pool_id="test-pool",
        vm_size=vm_size,
        container_name="test-run",
        mount_path="test-run",
        settings=settings,
    )

    pool = client.pool.add.call_args.args[0]
    profile = pool.virtual_machine_configuration.security_profile

    assert pool.vm_size == vm_size
    assert profile is not None
    assert profile.security_type == "trustedLaunch"
    assert profile.uefi_settings is None


def test_generate_sas_url_for_blob_and_container():
    blob_url = generate_sas_url(
        account_name="storage",
        account_domain="blob.core.windows.net",
        container_name="nanopore-runs",
        blob_name="1/manifests/abc.json",
        sas_token="token",
    )
    container_url = generate_sas_url(
        account_name="storage",
        account_domain="blob.core.windows.net",
        container_name="nanopore-runs",
        blob_name="",
        sas_token="token",
    )
    assert blob_url == (
        "https://storage.blob.core.windows.net/"
        "nanopore-runs/1/manifests/abc.json?token"
    )
    assert container_url == (
        "https://storage.blob.core.windows.net/nanopore-runs?token"
    )


def test_add_tasks_quotes_shell_command_safely():
    settings = Mock(
        conda_path="/usr/bin/miniconda/bin",
        runtime_bin_path="/usr/bin/miniconda/bin",
        mamba_root_prefix=None,
    )

    tasks = []
    sys_call = 'printf "%s" "hello world"'

    add_tasks(
        task_id="test-task",
        tasks=tasks,
        resource_input_files=[],
        resource_output_files=[],
        settings=settings,
        sys_call=sys_call,
    )

    assert len(tasks) == 1

    task = tasks[0]

    expected_wrapped_call = (
        'export PATH="${FOODPORT_RUNTIME_PATH}:${PATH}"; ' + sys_call
    )

    assert task.command_line == ("/bin/bash -c " + shlex.quote(expected_wrapped_call))

    environment = {item.name: item.value for item in task.environment_settings}

    assert environment == {
        "CONDA": "/usr/bin/miniconda/bin",
        "FOODPORT_RUNTIME_PATH": "/usr/bin/miniconda/bin",
    }


@patch("azure_batch.methods.generate_container_sas", return_value="sas-token")
def test_prep_output_container_returns_container_sas(mock_generate):
    client = Mock()
    settings = Mock(azure_account_name="storage", azure_account_key="key")
    result = prep_output_container("nanopore-runs", settings, client)
    client.create_container.assert_called_once_with(name="nanopore-runs")
    assert result == "https://storage.blob.core.windows.net/nanopore-runs?sas-token"
    assert mock_generate.call_args.kwargs["permission"].read is True
    assert mock_generate.call_args.kwargs["permission"].write is True


@patch("azure_batch.methods.generate_container_sas", return_value="sas-token")
def test_prep_output_container_accepts_existing_container(_mock_generate):
    client = Mock()
    client.create_container.side_effect = ResourceExistsError("exists")
    settings = Mock(azure_account_name="storage", azure_account_key="key")
    result = prep_output_container("nanopore-runs", settings, client)
    assert result.endswith("nanopore-runs?sas-token")


@patch(
    "azure_batch.methods.prep_output_container",
    return_value="https://storage/container?sas",
)
def test_log_output_resource_files_uploads_logs_on_completion(_mock_prep):
    outputs = log_output_resource_files(
        blob_storage_service_client=Mock(),
        output_files=[],
        settings=Mock(),
        output_container_name="nanopore-runs",
        log_prefix="1/logs",
    )
    assert len(outputs) == 2
    destinations = [item.destination.container.path for item in outputs]
    assert destinations == [
        str(Path("1/logs") / "azure_stderr.txt"),
        str(Path("1/logs") / "azure_stdout.txt"),
    ]
    for output in outputs:
        assert output.upload_options.upload_condition == (
            batchmodels.OutputFileUploadCondition.task_completion
        )


def test_parse_resource_input_pattern_adds_default_destination():
    patterns = [["source/file.pod5"], ["source/*.json", "manifests"]]
    assert parse_resource_input_pattern(patterns) == [
        ["source/file.pod5", ""],
        ["source/*.json", str(Path("manifests")) + "/"],
    ]


def test_parse_resource_input_pattern_rejects_extra_fields():
    with pytest.raises(SystemExit):
        parse_resource_input_pattern([["one", "two", "three"]])


def test_read_command_file_preserves_one_task_per_line(tmp_path):
    command_file = tmp_path / "commands.txt"
    command_file.write_text("first command\nsecond command\n", encoding="utf-8")
    assert read_command_file(str(command_file)) == [
        "first command",
        "second command",
    ]


def test_wait_for_tasks_to_complete_returns_when_all_complete():
    client = Mock()
    client.task.list.return_value = [
        Mock(state=batchmodels.TaskState.completed),
        Mock(state=batchmodels.TaskState.completed),
    ]
    assert (
        wait_for_tasks_to_complete(
            client,
            "job-id",
            datetime.timedelta(seconds=1),
        )
        is True
    )


def test_wait_for_tasks_to_complete_times_out():
    client = Mock()
    client.task.list.return_value = [Mock(state=batchmodels.TaskState.running)]
    with pytest.raises(RuntimeError, match="did not reach 'Completed'"):
        wait_for_tasks_to_complete(
            client,
            "job-id",
            datetime.timedelta(seconds=0),
        )


def test_add_tasks_configures_nanopore_micromamba_runtime():
    settings = Settings(
        settings_values(),
        "Nanopore",
    )

    tasks = []
    sys_call = "python scheduler.py input.csv metadata.csv"

    add_tasks(
        task_id="nanopore-task",
        tasks=tasks,
        resource_input_files=[],
        resource_output_files=[],
        settings=settings,
        sys_call=sys_call,
    )

    assert len(tasks) == 1

    task = tasks[0]

    environment = {
        item.name: item.value
        for item in task.environment_settings
    }

    assert environment["CONDA"] == "/opt/micromamba/bin"
    assert (
        environment["MAMBA_ROOT_PREFIX"]
        == "/opt/micromamba/root"
    )
    assert environment["FOODPORT_RUNTIME_PATH"] == (
        "/opt/micromamba/root/envs/poresippr/bin:"
        "/opt/micromamba/bin:"
        "/opt/ont/dorado/bin:"
        "/usr/local/bin:"
        "/usr/bin:"
        "/bin"
    )

    expected_wrapped_call = (
        'export PATH="${FOODPORT_RUNTIME_PATH}:${PATH}"; '
        + sys_call
    )

    assert task.command_line == (
        "/bin/bash -c "
        + shlex.quote(expected_wrapped_call)
    )


def test_nanopore_uses_configured_gpu_size():
    settings = Settings(
        settings_values(),
        "Nanopore",
    )
    client = Mock()

    create_pool(
        batch_service_client=client,
        pool_id="nanopore-pool",
        vm_size="",
        container_name="nanopore-run",
        mount_path="nanopore-run",
        settings=settings,
    )

    pool = client.pool.add.call_args.args[0]

    assert pool.vm_size == "Standard_NV18ads_A10_v5"


def test_explicit_vm_size_overrides_analysis_default():
    settings = Settings(
        settings_values(),
        "Nanopore",
    )
    client = Mock()

    create_pool(
        batch_service_client=client,
        pool_id="nanopore-pool",
        vm_size="Standard_D4ds_v5",
        container_name="nanopore-run",
        mount_path="nanopore-run",
        settings=settings,
    )

    pool = client.pool.add.call_args.args[0]

    assert pool.vm_size == "Standard_D4ds_v5"
