"""Unit tests for AzureBatch orchestration without contacting Azure."""

import datetime
from unittest.mock import Mock, patch

from azure_batch.azure_cli import AzureBatch


def make_batch(worker=True, no_tidy=False, unique_id="np-42-deadbeef"):
    """Build an AzureBatch instance without running its networked initializer."""
    item = AzureBatch.__new__(AzureBatch)
    item.start_time = datetime.datetime.now(tz=datetime.timezone.utc)
    item.blob_service_client = Mock()
    item.container = "nanopore-runs"
    item.upload_folder = None
    item.input_file_pattern = None
    item.bulk_input_file_pattern = None
    item.path = "/tmp"
    item.settings = Mock(
        vm_client_id="client-id",
        vm_secret="secret",
        vm_tenant="tenant",
        batch_account_url="https://batch.example.test",
    )
    item.unique_id = unique_id
    item.vm_size = "Standard_NV18ads_A10_v5"
    item.worker = worker
    item.download_file_pattern = None
    item.no_tidy = no_tidy
    item.log_prefix = "42/logs"
    item.sys_call = ["run_nanopore --manifest 42/manifests/abc.json"]
    item.logger = Mock()
    return item


@patch("azure_batch.azure_cli.jsonify", side_effect=lambda value: value)
@patch("azure_batch.azure_cli.log_output_resource_files", return_value=["logs"])
@patch("azure_batch.azure_cli.add_tasks")
@patch("azure_batch.azure_cli.create_job")
@patch("azure_batch.azure_cli.create_pool")
@patch("azure_batch.azure_cli.BatchServiceClient")
@patch("azure_batch.azure_cli.ServicePrincipalCredentials")
def test_worker_submission_returns_deterministic_identifiers(
    _mock_credentials,
    mock_client_class,
    mock_create_pool,
    mock_create_job,
    mock_add_tasks,
    _mock_logs,
    _mock_jsonify,
):
    batch_client = mock_client_class.return_value
    batch = make_batch()

    result = batch.main()

    assert result == {
        "pool_id": "nanopore-runs-np-42-deadbeef-pool",
        "job_id": "nanopore-runs-np-42-deadbeef-job",
        "tasks": ["nanopore-runs-np-42-deadbeef-task-0"],
        "status": "Success",
        "error": "",
    }
    mock_create_pool.assert_called_once_with(
        batch_service_client=batch_client,
        pool_id=result["pool_id"],
        vm_size="Standard_NV18ads_A10_v5",
        settings=batch.settings,
        container_name="nanopore-runs",
        mount_path="nanopore-runs",
    )
    mock_create_job.assert_called_once_with(
        batch_service_client=batch_client,
        job_id=result["job_id"],
        pool_id=result["pool_id"],
    )
    mock_add_tasks.assert_called_once_with(
        task_id=result["tasks"][0],
        tasks=[],
        resource_input_files=[],
        resource_output_files=["logs"],
        sys_call=batch.sys_call[0],
    )
    batch_client.task.add_collection.assert_called_once_with(
        job_id=result["job_id"], value=[]
    )
    batch_client.job.delete.assert_not_called()
    batch_client.pool.delete.assert_not_called()


@patch("azure_batch.azure_cli.jsonify", side_effect=lambda value: value)
@patch("azure_batch.azure_cli.log_output_resource_files", return_value=[])
@patch("azure_batch.azure_cli.add_tasks")
@patch("azure_batch.azure_cli.create_job")
@patch("azure_batch.azure_cli.create_pool")
@patch("azure_batch.azure_cli.BatchServiceClient")
@patch("azure_batch.azure_cli.ServicePrincipalCredentials")
def test_foodport_unique_id_uses_container_for_all_ids(
    _mock_credentials,
    mock_client_class,
    _mock_pool,
    _mock_job,
    _mock_add_tasks,
    _mock_logs,
    _mock_jsonify,
):
    batch = make_batch(unique_id="FoodPort")
    result = batch.main()
    assert result["pool_id"] == "nanopore-runs"
    assert result["job_id"] == "nanopore-runs"
    assert result["tasks"] == ["nanopore-runs-0"]
    mock_client_class.return_value.task.add_collection.assert_called_once()


@patch("azure_batch.azure_cli.download_files")
@patch("azure_batch.azure_cli.wait_for_tasks_to_complete")
@patch("azure_batch.azure_cli.log_output_resource_files", return_value=[])
@patch("azure_batch.azure_cli.add_tasks")
@patch("azure_batch.azure_cli.create_job")
@patch("azure_batch.azure_cli.create_pool")
@patch("azure_batch.azure_cli.BatchServiceClient")
@patch("azure_batch.azure_cli.ServicePrincipalCredentials")
def test_non_worker_waits_and_cleans_up(
    _mock_credentials,
    mock_client_class,
    _mock_pool,
    _mock_job,
    _mock_add_tasks,
    _mock_logs,
    mock_wait,
    mock_download,
):
    batch_client = mock_client_class.return_value
    batch = make_batch(worker=False, no_tidy=False)
    batch.download_file_pattern = [["results/"]]

    assert batch.main() is None

    mock_wait.assert_called_once()
    mock_download.assert_called_once_with(
        container_name="nanopore-runs",
        download_file_pattern=[["results/"]],
        path="/tmp",
        settings=batch.settings,
    )
    batch_client.job.delete.assert_called_once_with("nanopore-runs-np-42-deadbeef-job")
    batch_client.pool.delete.assert_called_once_with(
        "nanopore-runs-np-42-deadbeef-pool"
    )


@patch("azure_batch.azure_cli.wait_for_tasks_to_complete")
@patch("azure_batch.azure_cli.log_output_resource_files", return_value=[])
@patch("azure_batch.azure_cli.add_tasks")
@patch("azure_batch.azure_cli.create_job")
@patch("azure_batch.azure_cli.create_pool")
@patch("azure_batch.azure_cli.BatchServiceClient")
@patch("azure_batch.azure_cli.ServicePrincipalCredentials")
def test_no_tidy_skips_cleanup_without_raising_system_exit(
    _mock_credentials,
    mock_client_class,
    _mock_pool,
    _mock_job,
    _mock_add_tasks,
    _mock_logs,
    _mock_wait,
):
    batch = make_batch(worker=False, no_tidy=True)
    assert batch.main() is None
    mock_client_class.return_value.job.delete.assert_not_called()
    mock_client_class.return_value.pool.delete.assert_not_called()


@patch("azure_batch.azure_cli.BlobServiceClient")
@patch("azure_batch.azure_cli.validate_container_name")
@patch("azure_batch.azure_cli.read_command_file")
def test_initializer_builds_clients_and_reads_commands(
    mock_read_commands,
    mock_validate_container,
    mock_blob_client,
    tmp_path,
):
    command_file = tmp_path / "commands.txt"
    command_file.write_text("run nanopore\n", encoding="utf-8")
    mock_read_commands.return_value = ["run nanopore"]
    mock_validate_container.return_value = "nanopore-runs"
    settings = Mock(
        azure_account_name="storage",
        azure_account_key="key",
    )

    batch = AzureBatch(
        command_file=str(command_file),
        vm_size="Standard_NV18ads_A10_v5",
        settings=settings,
        container="nanopore-runs",
        path=str(tmp_path),
        unique_id="np-42-deadbeef",
        worker=True,
        log_prefix="42/logs",
    )

    mock_read_commands.assert_called_once_with(command_file=str(command_file))
    mock_validate_container.assert_called_once_with(container_name="nanopore-runs")
    mock_blob_client.assert_called_once_with(
        account_url="https://storage.blob.core.windows.net/",
        credential="key",
    )
    assert batch.sys_call == ["run nanopore"]
    assert batch.unique_id == "np-42-deadbeef"
    assert batch.log_prefix == "42/logs"


@patch("azure_batch.azure_cli.copy_blobs_to_container")
@patch("azure_batch.azure_cli.match_file_and_expression")
@patch("azure_batch.azure_cli.parse_resource_file_list")
@patch("azure_batch.azure_cli.prep_resource_files")
@patch("azure_batch.azure_cli.parse_resource_input_pattern")
@patch("azure_batch.azure_cli.read_bulk_input_pattern")
@patch("azure_batch.azure_cli.jsonify", side_effect=lambda value: value)
@patch("azure_batch.azure_cli.log_output_resource_files", return_value=[])
@patch("azure_batch.azure_cli.add_tasks")
@patch("azure_batch.azure_cli.create_job")
@patch("azure_batch.azure_cli.create_pool")
@patch("azure_batch.azure_cli.BatchServiceClient")
@patch("azure_batch.azure_cli.ServicePrincipalCredentials")
def test_bulk_input_is_prepared_before_submission(
    _mock_credentials,
    _mock_client,
    _mock_pool,
    _mock_job,
    _mock_add_tasks,
    _mock_logs,
    _mock_jsonify,
    mock_read_bulk,
    mock_parse_patterns,
    mock_prep_files,
    mock_parse_files,
    mock_match,
    mock_copy,
    tmp_path,
):
    batch = make_batch()
    batch.path = str(tmp_path)
    batch.bulk_input_file_pattern = str(tmp_path / "input.txt")
    mock_read_bulk.return_value = [["source/*.pod5"]]
    mock_parse_patterns.return_value = [["source/*.pod5", ""]]
    mock_parse_files.return_value = [["source", "file.pod5"]]
    mock_match.return_value = [["source", "file.pod5", ""]]

    batch.main()

    mock_read_bulk.assert_called_once_with(
        bulk_input_file_pattern=batch.bulk_input_file_pattern
    )
    mock_prep_files.assert_called_once()
    mock_copy.assert_called_once_with(
        blob_service_client=batch.blob_service_client,
        container_name="nanopore-runs",
        resource_files_with_output=[["source", "file.pod5", ""]],
        settings=batch.settings,
    )
