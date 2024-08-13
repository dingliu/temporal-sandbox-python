import asyncio
import aiofiles
import yaml
import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import List
from uuid import uuid4

from temporalio import activity, workflow
from temporalio.common import RetryPolicy
from temporalio.client import Client
from temporalio.worker import Worker


@dataclass
class AzureVirtualNetworkType:
    name: str
    allowed_delegations: List[str]


@activity.defn
async def get_vnet_types() -> List[AzureVirtualNetworkType]:
    activity.heartbeat("Getting Azure Virtual Network Types")
    SAMPLE_FILEPATH = "sample.yaml"
    TOP_LEVEL_KEY = "AzureVirtualNetworkTypes"
    vnet_types = []


    async with aiofiles.open(SAMPLE_FILEPATH, mode="r") as f:
        sample_dict: dict = yaml.safe_load(await f.read())
        activity.logger.info(f"Sample dict: {sample_dict}")
    
    for k, v in sample_dict[TOP_LEVEL_KEY].items():
        vnet_types.append(AzureVirtualNetworkType(name=k, allowed_delegations=v["allowed_delegations"]))
    
    activity.logger.info(f"VNet Types: {vnet_types}")
    return vnet_types


@workflow.defn
class SampleWorkflow:

    async def _get_vnet_types(self) -> List[AzureVirtualNetworkType]:
        vnet_types = await workflow.execute_activity(
            activity=get_vnet_types,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(maximum_attempts=1)
        )
        return vnet_types

    @workflow.run
    async def run(self) -> List[AzureVirtualNetworkType]:
        vnet_types = await self._get_vnet_types()
        workflow.logger.info(f"VNet Types: {vnet_types}")
        return vnet_types

async def main():
    logging.basicConfig(level=logging.INFO)

    client = await Client.connect("localhost:7233")

    async with Worker(
        client,
        task_queue=f"tq-{uuid4()}",
        activities=[get_vnet_types],
        workflows=[SampleWorkflow],
    ) as worker:
        result = await client.execute_workflow(
            SampleWorkflow.run,
            id=f"wf-{uuid4()}",
            task_queue=worker.task_queue,
        )
        logging.info(f"Workflow complete: {result}")

if __name__ == "__main__":
    asyncio.run(main())
