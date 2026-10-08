from pathlib import Path
import os
from typing import Optional
from temporalio.service import TLSConfig


def create_tls_config(
    keyfile: Optional[str] = None,
    certfile: Optional[str] = None,
    ca_cert: Optional[str] = None,
    client_cert_content: Optional[str | bytes] = None,
    client_private_key_content: Optional[str | bytes] = None,
    ca_cert_content: Optional[str | bytes] = None,
    verification_server_name: Optional[str] = None,
    domain: Optional[str] = None,
) -> TLSConfig:
    """
    Creates Temporal TLSConfig.
    Supports either:
    1. Direct PEM content (bytes or string)
    2. File paths (defaulting to 'secrets/' directory)
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(base_dir, ".."))
    secrets_dir = os.path.join(project_root, "secrets")

    # Client certificate
    cert_bytes: bytes | None = None
    if client_cert_content:
        cert_bytes = (
            client_cert_content.encode("utf-8")
            if isinstance(client_cert_content, str)
            else client_cert_content
        )
    else:
        certfile = certfile or os.path.join(secrets_dir, "cert.pem")
        if os.path.exists(certfile):
            with open(certfile, "rb") as f:
                cert_bytes = f.read()
        else:
            raise FileNotFoundError(f"Client certificate not found at: {certfile}")

    # Private key
    key_bytes: bytes | None = None
    if client_private_key_content:
        key_bytes = (
            client_private_key_content.encode("utf-8")
            if isinstance(client_private_key_content, str)
            else client_private_key_content
        )
    else:
        keyfile = keyfile or os.path.join(secrets_dir, "key.pem")
        if os.path.exists(keyfile):
            with open(keyfile, "rb") as f:
                key_bytes = f.read()
        else:
            raise FileNotFoundError(f"Client private key not found at: {keyfile}")

    # CA Root certificate
    ca_bytes: bytes | None = None
    if ca_cert_content:
        ca_bytes = (
            ca_cert_content.encode("utf-8")
            if isinstance(ca_cert_content, str)
            else ca_cert_content
        )
    else:
        ca_cert = ca_cert or os.path.join(secrets_dir, "ca-chain.pem")
        if os.path.exists(ca_cert):
            with open(ca_cert, "rb") as f:
                ca_bytes = f.read()
        # CA root is optional in some public CA setups, but if ca_cert was explicitly passed or default exists, use it.

    return TLSConfig(
        domain=domain or None,
        server_root_ca_cert=ca_bytes,
        client_cert=cert_bytes,
        client_private_key=key_bytes,
        verification_server_name=verification_server_name or None,
    )
