import asyncio
import logging
from temporalio import workflow
from temporalio.client import Client
from temporalio.worker import Worker

from app.config import load_connection_config
from app.tls_config import create_tls_config

logging.basicConfig(level=logging.INFO)


@workflow.defn(name="BaseWorkflow.run")
class BaseWorkflow:
    """Demo workflow matching the user's workflow type 'BaseWorkflow.run'."""

    @workflow.run
    async def run(self, *args) -> str:
        workflow.logger.info(f"BaseWorkflow execution started with args: {args}")
        # Emulate some processing time
        for i in range(15):
            await asyncio.sleep(1)
            workflow.logger.info(f"Working step {i + 1}/15...")
        return f"Done with args: {args}"


async def main():
    config = load_connection_config()
    tls = None
    if config.tls_enabled:
        tls = create_tls_config(
            keyfile=config.keyfile,
            certfile=config.certfile,
            ca_cert=config.ca_cert,
            verification_server_name=config.verification_server_name or None,
            domain=config.domain or None,
        )

    print(f"Connecting worker to Temporal at {config.host} (namespace: {config.namespace})...")
    client = await Client.connect(
        target_host=config.host,
        namespace=config.namespace,
        tls=tls,
    )

    worker = Worker(
        client,
        task_queue="default",
        workflows=[BaseWorkflow],
    )
    print("Worker started on task queue 'default'. Press Ctrl+C to exit.")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
