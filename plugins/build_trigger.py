import asyncio

from airflow.triggers.base import BaseTrigger, TriggerEvent


class BuildTrigger(BaseTrigger):
    def __init__(self):
        super().__init__()

    def serialize(self):
        return ("airflow.triggers.temporal.BuildTrigger")

    async def run(self):
        while self.moment > timezone.utcnow():
            await asyncio.sleep(1)
        yield TriggerEvent(self.moment)