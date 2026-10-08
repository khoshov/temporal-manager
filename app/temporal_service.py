import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from temporalio.client import Client, WorkflowExecutionStatus
from app.config import ConnectionConfig, load_connection_config, save_connection_config
from app.tls_config import create_tls_config

logger = logging.getLogger("temporal-manager")


class TemporalService:
    def __init__(self):
        self.client: Optional[Client] = None
        self.config: ConnectionConfig = load_connection_config()
        self.is_connected: bool = False
        self.last_error: Optional[str] = None
        self.last_connected_at: Optional[datetime] = None

    async def connect(self, config: Optional[ConnectionConfig] = None) -> bool:
        """Connect or reconnect to the Temporal cluster."""
        if config:
            self.config = config

        self.is_connected = False
        self.last_error = None

        tls_config = None
        if self.config.tls_enabled:
            try:
                tls_config = create_tls_config(
                    keyfile=self.config.keyfile,
                    certfile=self.config.certfile,
                    ca_cert=self.config.ca_cert,
                    client_cert_content=self.config.client_cert_pem,
                    client_private_key_content=self.config.client_key_pem,
                    ca_cert_content=self.config.ca_cert_pem,
                    verification_server_name=self.config.verification_server_name or None,
                    domain=self.config.domain or None,
                )
            except Exception as e:
                self.last_error = f"TLS Error: {str(e)}"
                logger.error(f"Failed to create TLS configuration: {e}")
                return False

        try:
            logger.info(f"Connecting to Temporal at {self.config.host} (namespace: {self.config.namespace})...")
            # Connect with a timeout
            self.client = await asyncio.wait_for(
                Client.connect(
                    target_host=self.config.host,
                    namespace=self.config.namespace,
                    tls=tls_config,
                ),
                timeout=10.0,
            )
            self.is_connected = True
            self.last_connected_at = datetime.now()
            # Save configuration if connection succeeded
            save_connection_config(self.config)
            logger.info("Successfully connected to Temporal!")
            return True
        except asyncio.TimeoutError:
            self.last_error = f"Connection timed out after 10s connecting to {self.config.host}"
            logger.error(self.last_error)
            return False
        except Exception as e:
            self.last_error = str(e)
            logger.error(f"Failed to connect to Temporal: {e}")
            return False

    def require_client(self) -> Client:
        if not self.is_connected or not self.client:
            raise RuntimeError(self.last_error or "Not connected to Temporal server")
        return self.client

    async def list_workflows(
        self,
        query: Optional[str] = None,
        status: Optional[str] = None,
        search: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """List workflows matching the given filter or Temporal query."""
        client = self.require_client()

        # Build Temporal SQL-like visibility query if needed
        query_parts = []
        if query and query.strip():
            query_parts.append(query.strip())
        else:
            if status and status.upper() != "ALL":
                status_map = {
                    "RUNNING": 'ExecutionStatus = "Running"',
                    "COMPLETED": 'ExecutionStatus = "Completed"',
                    "FAILED": 'ExecutionStatus = "Failed"',
                    "CANCELED": 'ExecutionStatus = "Canceled"',
                    "TERMINATED": 'ExecutionStatus = "Terminated"',
                    "TIMED_OUT": 'ExecutionStatus = "TimedOut"',
                    "CONTINUED_AS_NEW": 'ExecutionStatus = "ContinuedAsNew"',
                }
                mapped = status_map.get(status.upper())
                if mapped:
                    query_parts.append(mapped)

            if search and search.strip():
                s = search.strip().replace('"', '\\"')
                query_parts.append(f'(WorkflowId STARTS_WITH "{s}" OR WorkflowType STARTS_WITH "{s}")')

        final_query = " AND ".join(query_parts) if query_parts else None

        results = []
        try:
            async for wf in client.list_workflows(final_query, limit=limit):
                status_str = "UNKNOWN"
                if wf.status is not None:
                    status_str = wf.status.name if hasattr(wf.status, "name") else str(wf.status)

                results.append({
                    "id": wf.id,
                    "run_id": wf.run_id,
                    "workflow_type": wf.workflow_type,
                    "task_queue": wf.task_queue,
                    "status": status_str,
                    "start_time": wf.start_time.isoformat() if wf.start_time else None,
                    "close_time": wf.close_time.isoformat() if wf.close_time else None,
                    "execution_time": wf.execution_time.isoformat() if wf.execution_time else None,
                    "history_length": wf.history_length,
                })
                if len(results) >= limit:
                    break
        except Exception as e:
            logger.error(f"Error listing workflows: {e}")
            raise e

        return results

    async def describe_workflow(self, workflow_id: str, run_id: Optional[str] = None) -> Dict[str, Any]:
        """Describe detailed status of a workflow."""
        client = self.require_client()
        handle = client.get_workflow_handle(workflow_id, run_id=run_id)
        desc = await handle.describe()

        status_str = "UNKNOWN"
        if desc.status is not None:
            status_str = desc.status.name if hasattr(desc.status, "name") else str(desc.status)

        return {
            "id": desc.id,
            "run_id": desc.run_id,
            "workflow_type": desc.workflow_type,
            "task_queue": desc.task_queue,
            "status": status_str,
            "start_time": desc.start_time.isoformat() if desc.start_time else None,
            "close_time": desc.close_time.isoformat() if desc.close_time else None,
            "execution_time": desc.execution_time.isoformat() if desc.execution_time else None,
            "history_length": desc.history_length,
            "parent_id": desc.parent_id,
            "parent_run_id": desc.parent_run_id,
        }

    async def start_workflow(
        self,
        workflow_type: str,
        workflow_id: str,
        task_queue: str,
        args: Optional[List[Any]] = None,
        cancel_after_seconds: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Start a new workflow and optionally auto-cancel after delay."""
        client = self.require_client()
        args = args or []

        handle = await client.start_workflow(
            workflow=workflow_type,
            args=args,
            id=workflow_id,
            task_queue=task_queue,
        )

        async def _auto_cancel(h, delay_sec: float):
            await asyncio.sleep(delay_sec)
            try:
                await h.cancel()
                logger.info(f"Auto-cancelled workflow {workflow_id} after {delay_sec}s")
            except Exception as ex:
                logger.warning(f"Auto-cancel workflow {workflow_id} failed: {ex}")

        if cancel_after_seconds and cancel_after_seconds > 0:
            asyncio.create_task(_auto_cancel(handle, cancel_after_seconds))

        return {
            "id": handle.id,
            "run_id": handle.result_run_id or getattr(handle, "first_execution_run_id", None),
            "status": "STARTED",
        }

    async def cancel_workflow(self, workflow_id: str, run_id: Optional[str] = None) -> bool:
        """Cancel a running workflow."""
        client = self.require_client()
        handle = client.get_workflow_handle(workflow_id, run_id=run_id)
        await handle.cancel()
        return True

    async def terminate_workflow(
        self, workflow_id: str, run_id: Optional[str] = None, reason: str = "Terminated via Web UI"
    ) -> bool:
        """Terminate a workflow."""
        client = self.require_client()
        handle = client.get_workflow_handle(workflow_id, run_id=run_id)
        await handle.terminate(reason=reason)
        return True


temporal_service = TemporalService()
