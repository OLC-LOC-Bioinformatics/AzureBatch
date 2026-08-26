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
    check_image_security_requirements,
    generate_sas_url,
    log_output_resource_files,
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


def test_settings_rejects_unsupported_analysis_type():
    with pytest.raises(ValueError, match="Unsupported analysis type"):
        Settings(settings_values(), "Unknown")


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
        "https://storage.blob.core.windows.net/nanopore-runs/1/manifests/abc.json?token"
    )
    assert container_url == (
        "https://storage.blob.core.windows.net/nanopore-runs?token"
    )


def test_add_tasks_quotes_shell_command_safely():
    tasks = []
    command = 'printf "%s" "hello world"'
    add_tasks(
        task_id="nanopore-task-0",
        tasks=tasks,
        resource_input_files=[],
        resource_output_files=[],
        sys_call=command,
    )
    assert len(tasks) == 1
    assert tasks[0].command_line == f"/bin/bash -c {shlex.quote(command)}"
    assert tasks[0].constraints.max_wall_clock_time == "PT16H"
    assert tasks[0].user_identity.auto_user.elevation_level == (
        batchmodels.ElevationLevel.admin
    )


@patch("azure_batch.methods.generate_container_sas", return_value="sas-token")
def test_prep_output_container_returns_container_sas(mock_generate):
    client = Mock()
    settings = Mock(azure_account_name="storage", azure_account_key="key")
    result = prep_output_container("nanopore-runs", settings, client)
    client.create_container.assert_called_once_with(name="nanopore-runs")
    assert result == ("https://storage.blob.core.windows.net/nanopore-runs?sas-token")
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
        wait_for_tasks_to_complete(client, "job-id", datetime.timedelta(seconds=1))
        is True
    )


def test_wait_for_tasks_to_complete_times_out():
    client = Mock()
    client.task.list.return_value = [Mock(state=batchmodels.TaskState.running)]
    with pytest.raises(RuntimeError, match="did not reach 'Completed'"):
        wait_for_tasks_to_complete(client, "job-id", datetime.timedelta(seconds=0))


@patch("azure_batch.methods.ComputeManagementClient")
@patch("azure_batch.methods.ClientSecretCredential")
def test_image_security_uses_gallery_trusted_launch(
    _mock_credential, mock_compute_client
):
    image = Mock()
    feature = Mock()
    feature.name = "SecurityType"
    feature.value = "TrustedLaunch"
    image.features = [feature]
    image.os_type = "Linux"
    mock_compute_client.return_value.gallery_images.get.return_value = image
    settings = Mock(
        vm_image=(
            "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/"
            "galleries/development/images/nanopore/versions/0.0.1"
        ),
        vm_tenant="tenant",
        vm_client_id="client",
        vm_secret="secret",
    )
    result = check_image_security_requirements(settings=settings)
    assert result["supports_trusted_launch"] is True
    assert result["recommended_security_profile"] == {"security_type": "trustedLaunch"}
