"""Unit tests for AzureBatch orchestration without contacting Azure."""

import datetime
from unittest.mock import Mock, patch

from azure_batch.azure_cli import (
    AzureBatch,
    _delete_batch_resources,
    _task_collection_errors,
)


def append_mock_task(task_id, tasks, **_kwargs):
    """Make mocked add_tasks behave like the production helper."""
    tasks.append(Mock(id=task_id))
    return tasks


def test_task_collection_errors_accepts_value_wrapped_results():
    """Support Azure SDK responses that expose task results via value."""
    successful = Mock(status="Success", task_id="task-1", error=None)
    failed = Mock(
        status="ClientError",
        task_id="task-2",
        error=Mock(code="InvalidTask", message="bad command"),
    )
    response = Mock(value=[successful, failed])

    assert _task_collection_errors(response) == [
        "task-2: InvalidTask: bad command"
    ]


def test_delete_batch_resources_preserves_all_cleanup_errors():
    """Report cleanup failures without hiding either resource failure."""
    client = Mock()
    client.job.delete.side_effect = RuntimeError("job cleanup failed")
    client.pool.delete.side_effect = RuntimeError("pool cleanup failed")

    errors = _delete_batch_resources(
        client,
        "job-1",
        "pool-1",
        job_created=True,
        pool_created=True,
        logger=Mock(),
    )

    assert errors == [
        "job job-1: job cleanup failed",
        "pool pool-1: pool cleanup failed",
    ]


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
    item.output_file_pattern = []
    item.output_container = "nanopore-runs"
    item.output_prefix = ""
    item.no_tidy = no_tidy
    item.log_prefix = "42/logs"
    item.sys_call = ["run_nanopore --manifest 42/manifests/abc.json"]
    item.logger = Mock()
    return item


@patch("azure_batch.azure_cli.log_output_resource_files", return_value=["logs"])
@patch(
    "azure_batch.azure_cli.add_tasks",
    side_effect=append_mock_task,
)
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
        "cleanup_errors": [],
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
    mock_add_tasks.assert_called_once()
    add_call = mock_add_tasks.call_args.kwargs
    assert add_call["task_id"] == result["tasks"][0]
    assert len(add_call["tasks"]) == 1
    assert add_call["tasks"][0].id == result["tasks"][0]
    assert add_call["resource_input_files"] == []
    assert add_call["resource_output_files"] == ["logs"]
    assert add_call["settings"] is batch.settings
    assert add_call["sys_call"] == batch.sys_call[0]

    batch_client.task.add_collection.assert_called_once()
    collection_call = batch_client.task.add_collection.call_args.kwargs
    assert collection_call["job_id"] == result["job_id"]
    assert len(collection_call["value"]) == 1
    assert collection_call["value"][0].id == result["tasks"][0]
    batch_client.job.delete.assert_not_called()
    batch_client.pool.delete.assert_not_called()


@patch("azure_batch.azure_cli.log_output_resource_files", return_value=[])
@patch(
    "azure_batch.azure_cli.add_tasks",
    side_effect=append_mock_task,
)
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
@patch(
    "azure_batch.azure_cli.add_tasks",
    side_effect=append_mock_task,
)
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
@patch(
    "azure_batch.azure_cli.add_tasks",
    side_effect=append_mock_task,
)
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


@patch("azure_batch.azure_cli.prepare_output_resource_files")
@patch("azure_batch.azure_cli.log_output_resource_files", return_value=[])
@patch(
    "azure_batch.azure_cli.add_tasks",
    side_effect=append_mock_task,
)
@patch("azure_batch.azure_cli.create_job")
@patch("azure_batch.azure_cli.create_pool")
@patch("azure_batch.azure_cli.BatchServiceClient")
@patch("azure_batch.azure_cli.ServicePrincipalCredentials")
def test_worker_submission_adds_outputs_to_separate_container(
    _mock_credentials,
    mock_client_class,
    _mock_pool,
    _mock_job,
    _mock_add_tasks,
    mock_logs,
    mock_prepare_output,
):
    batch = make_batch()
    batch.output_file_pattern = ["output/"]
    batch.output_container = "nanopore-results"
    batch.output_prefix = "runs/example"

    result = batch.main()

    assert result["status"] == "Success"
    assert result["cleanup_errors"] == []
    mock_logs.assert_called_once_with(
        blob_storage_service_client=batch.blob_service_client,
        output_files=[],
        settings=batch.settings,
        output_container_name="nanopore-results",
        log_prefix="42/logs",
    )
    mock_prepare_output.assert_called_once_with(
        blob_storage_service_client=batch.blob_service_client,
        output_item="output/",
        output_files=[],
        settings=batch.settings,
        output_container_name="nanopore-results",
        destination_prefix="runs/example",
    )


@patch("azure_batch.azure_cli.copy_blobs_to_container")
@patch("azure_batch.azure_cli.match_file_and_expression")
@patch("azure_batch.azure_cli.parse_resource_file_list")
@patch("azure_batch.azure_cli.prep_resource_files")
@patch("azure_batch.azure_cli.parse_resource_input_pattern")
@patch("azure_batch.azure_cli.read_bulk_input_pattern")
@patch("azure_batch.azure_cli.log_output_resource_files", return_value=[])
@patch(
    "azure_batch.azure_cli.add_tasks",
    side_effect=append_mock_task,
)
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
