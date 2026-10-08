import os
from pathlib import Path
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from pydantic import BaseModel, Field

from app.config import ConnectionConfig, SECRETS_DIR
from app.temporal_service import temporal_service

router = APIRouter(prefix="/api")


class ConfigUpdateRequest(BaseModel):
    host: str
    namespace: str = "default"
    tls_enabled: bool = False
    verification_server_name: Optional[str] = ""
    domain: Optional[str] = ""
    keyfile: Optional[str] = ""
    certfile: Optional[str] = ""
    ca_cert: Optional[str] = ""
    client_cert_pem: Optional[str] = None
    client_key_pem: Optional[str] = None
    ca_cert_pem: Optional[str] = None


class StartWorkflowRequest(BaseModel):
    workflow_type: str = Field(..., description="e.g. BaseWorkflow.run")
    workflow_id: str = Field(..., description="Unique ID for the workflow execution")
    task_queue: str = Field(..., description="Task queue name")
    args: Optional[List[Any]] = Field(default_factory=list, description="Positional arguments")
    cancel_after_seconds: Optional[float] = Field(
        default=None, description="Automatically cancel after N seconds if specified"
    )


class TerminateWorkflowRequest(BaseModel):
    reason: Optional[str] = Field(default="Terminated from UI")


@router.get("/status")
async def get_status():
    """Return current connection status and cluster info."""
    return {
        "connected": temporal_service.is_connected,
        "host": temporal_service.config.host,
        "namespace": temporal_service.config.namespace,
        "tls_enabled": temporal_service.config.tls_enabled,
        "last_connected_at": (
            temporal_service.last_connected_at.isoformat()
            if temporal_service.last_connected_at
            else None
        ),
        "last_error": temporal_service.last_error,
    }


@router.get("/config")
async def get_config():
    """Get current configuration, file existence checks, and connection state."""
    cfg = temporal_service.config

    cert_exists = os.path.exists(cfg.certfile) if cfg.certfile else False
    key_exists = os.path.exists(cfg.keyfile) if cfg.keyfile else False
    ca_exists = os.path.exists(cfg.ca_cert) if cfg.ca_cert else False

    return {
        "host": cfg.host,
        "namespace": cfg.namespace,
        "tls_enabled": cfg.tls_enabled,
        "verification_server_name": cfg.verification_server_name or "",
        "domain": cfg.domain or "",
        "certfile": cfg.certfile or "",
        "keyfile": cfg.keyfile or "",
        "ca_cert": cfg.ca_cert or "",
        "has_certfile": cert_exists or bool(cfg.client_cert_pem),
        "has_keyfile": key_exists or bool(cfg.client_key_pem),
        "has_ca_cert": ca_exists or bool(cfg.ca_cert_pem),
        "has_cert_pem": bool(cfg.client_cert_pem),
        "has_key_pem": bool(cfg.client_key_pem),
        "has_ca_pem": bool(cfg.ca_cert_pem),
        "is_connected": temporal_service.is_connected,
        "last_error": temporal_service.last_error,
    }


@router.post("/config")
async def update_config(payload: ConfigUpdateRequest):
    """Save configuration and attempt to connect."""
    # Build updated ConnectionConfig
    new_config = ConnectionConfig(
        host=payload.host.strip(),
        namespace=payload.namespace.strip(),
        tls_enabled=payload.tls_enabled,
        verification_server_name=payload.verification_server_name.strip() if payload.verification_server_name else "",
        domain=payload.domain.strip() if payload.domain else "",
        keyfile=payload.keyfile.strip() if payload.keyfile else str(SECRETS_DIR / "key.pem"),
        certfile=payload.certfile.strip() if payload.certfile else str(SECRETS_DIR / "cert.pem"),
        ca_cert=payload.ca_cert.strip() if payload.ca_cert else str(SECRETS_DIR / "ca-chain.pem"),
        client_cert_pem=payload.client_cert_pem if payload.client_cert_pem and payload.client_cert_pem.strip() else None,
        client_key_pem=payload.client_key_pem if payload.client_key_pem and payload.client_key_pem.strip() else None,
        ca_cert_pem=payload.ca_cert_pem if payload.ca_cert_pem and payload.ca_cert_pem.strip() else None,
    )

    success = await temporal_service.connect(new_config)
    if not success:
        return {
            "success": False,
            "connected": False,
            "error": temporal_service.last_error or "Failed to connect",
        }

    return {
        "success": True,
        "connected": True,
        "error": None,
    }


@router.post("/upload-cert")
async def upload_certificate_file(
    cert_type: str = Form(..., description="'cert', 'key', or 'ca'"),
    file: UploadFile = File(...),
):
    """Upload a certificate or key file to secrets/."""
    SECRETS_DIR.mkdir(parents=True, exist_ok=True)
    filename_map = {
        "cert": "cert.pem",
        "key": "key.pem",
        "ca": "ca-chain.pem",
    }
    target_filename = filename_map.get(cert_type)
    if not target_filename:
        raise HTTPException(status_code=400, detail="Invalid cert_type. Must be 'cert', 'key', or 'ca'.")

    dest_path = SECRETS_DIR / target_filename
    content = await file.read()
    with open(dest_path, "wb") as f:
        f.write(content)

    # Update path in current config
    if cert_type == "cert":
        temporal_service.config.certfile = str(dest_path)
    elif cert_type == "key":
        temporal_service.config.keyfile = str(dest_path)
    elif cert_type == "ca":
        temporal_service.config.ca_cert = str(dest_path)

    return {
        "success": True,
        "file": target_filename,
        "path": str(dest_path),
        "size_bytes": len(content),
    }


@router.get("/workflows")
async def list_workflows(
    query: Optional[str] = None,
    status: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 50,
):
    """List workflows with filtering."""
    if not temporal_service.is_connected:
        raise HTTPException(
            status_code=503,
            detail=temporal_service.last_error or "Не подключено к серверу Temporal. Проверьте настройки подключения.",
        )

    try:
        workflows = await temporal_service.list_workflows(
            query=query, status=status, search=search, limit=limit
        )
        return {"workflows": workflows}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/workflows/{workflow_id}")
async def describe_workflow(workflow_id: str, run_id: Optional[str] = None):
    """Get details of a workflow."""
    if not temporal_service.is_connected:
        raise HTTPException(status_code=503, detail="Not connected to Temporal")
    try:
        desc = await temporal_service.describe_workflow(workflow_id, run_id=run_id)
        return desc
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/workflows/start")
async def start_workflow(payload: StartWorkflowRequest):
    """Start a new workflow execution."""
    if not temporal_service.is_connected:
        raise HTTPException(
            status_code=503,
            detail=temporal_service.last_error or "Не подключено к серверу Temporal",
        )

    try:
        result = await temporal_service.start_workflow(
            workflow_type=payload.workflow_type,
            workflow_id=payload.workflow_id,
            task_queue=payload.task_queue,
            args=payload.args or [],
            cancel_after_seconds=payload.cancel_after_seconds,
        )
        return {"success": True, **result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Ошибка запуска воркфлоу: {str(e)}")


@router.post("/workflows/{workflow_id}/cancel")
async def cancel_workflow(workflow_id: str, run_id: Optional[str] = None):
    """Request cancellation for a running workflow."""
    if not temporal_service.is_connected:
        raise HTTPException(status_code=503, detail="Not connected to Temporal")

    try:
        await temporal_service.cancel_workflow(workflow_id, run_id=run_id)
        return {"success": True, "message": f"Workflow {workflow_id} cancellation requested"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/workflows/{workflow_id}/terminate")
async def terminate_workflow(
    workflow_id: str, run_id: Optional[str] = None, payload: TerminateWorkflowRequest = None
):
    """Terminate a workflow."""
    if not temporal_service.is_connected:
        raise HTTPException(status_code=503, detail="Not connected to Temporal")

    reason = payload.reason if payload else "Terminated from UI"
    try:
        await temporal_service.terminate_workflow(workflow_id, run_id=run_id, reason=reason)
        return {"success": True, "message": f"Workflow {workflow_id} terminated"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
