from typing import Optional
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from hooks.ec2_instance_hooks import EC2ExtendedHooks


class EC2ExtendedTerminateInstance(BaseOperator):
    """
    Terminate EC2 instance filtered by id.

    :param instance_id: id of the AWS EC2 instance
    :type instance_id: str
    :return: None
    """
    template_fields = ["instance_id", "region_name"]
    ui_color = "#ace1af"
    ui_fgcolor = "#00563f"

    @apply_defaults
    def __init__(self, instance_id: str, aws_conn_id: str = "aws_default", region_name: Optional[str] = None, check_interval: float = 15, **kwargs):
        super(EC2ExtendedTerminateInstance, self).__init__(**kwargs)
        self.instance_id = instance_id
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.check_interval = check_interval

    def execute(self, context: 'Context'):
        EC2Instance = EC2ExtendedHooks(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name
        )
        self.log.info("Terminating instance %s", self.instance_id)
        EC2Instance.terminate_instance(self.instance_id, self.region_name)
