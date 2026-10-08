import json
import os
from pathlib import Path
from typing import Optional
from pydantic import BaseModel, Field


BASE_DIR = Path(__file__).resolve().parent.parent
SECRETS_DIR = BASE_DIR / "secrets"
CONFIG_FILE = SECRETS_DIR / "connection_config.json"


class ConnectionConfig(BaseModel):
    host: str = Field(default="localhost:7233")
    namespace: str = Field(default="default")
    tls_enabled: bool = Field(default=False)
    verification_server_name: Optional[str] = Field(default="")
    domain: Optional[str] = Field(default="")
    keyfile: Optional[str] = Field(default=str(SECRETS_DIR / "key.pem"))
    certfile: Optional[str] = Field(default=str(SECRETS_DIR / "cert.pem"))
    ca_cert: Optional[str] = Field(default=str(SECRETS_DIR / "ca-chain.pem"))
    client_cert_pem: Optional[str] = Field(default=None)
    client_key_pem: Optional[str] = Field(default=None)
    ca_cert_pem: Optional[str] = Field(default=None)


def load_connection_config() -> ConnectionConfig:
    SECRETS_DIR.mkdir(parents=True, exist_ok=True)
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return ConnectionConfig(**data)
        except Exception:
            pass

    # Fallback to environment variables
    tls_enabled = os.getenv("TEMPORAL_TLS_ENABLED", "false").lower() in ("true", "1", "yes")
    return ConnectionConfig(
        host=os.getenv("TEMPORAL_HOST", "localhost:7233"),
        namespace=os.getenv("TEMPORAL_NAMESPACE", "default"),
        tls_enabled=tls_enabled,
        verification_server_name=os.getenv("TEMPORAL_TLS_VERIFICATION_SERVER_NAME", ""),
        domain=os.getenv("TEMPORAL_TLS_DOMAIN", ""),
        keyfile=os.getenv("TEMPORAL_TLS_KEYFILE", str(SECRETS_DIR / "key.pem")),
        certfile=os.getenv("TEMPORAL_TLS_CERTFILE", str(SECRETS_DIR / "cert.pem")),
        ca_cert=os.getenv("TEMPORAL_TLS_CA_CERT", str(SECRETS_DIR / "ca-chain.pem")),
    )


def save_connection_config(config: ConnectionConfig) -> None:
    SECRETS_DIR.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        # Don't necessarily expose full sensitive PEM in plain json if user wants files,
        # but storing here enables seamless configuration via UI.
        f.write(config.model_dump_json(indent=2))
