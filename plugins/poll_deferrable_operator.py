from airflow.models.baseoperator import BaseOperator


class PollDeferrableOperator(BaseOperator):
    def __init__(self, latest_build, installed_build, **kwargs) -> None:
        super().__init__(**kwargs)
        self.latest_build = latest_build
        self.installed_build = installed_build

    def execute(self, context):
        pass