from typing import List, Dict, Optional
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.ec2 import EC2Hook
from hooks.ec2_instance_hooks import EC2ExtendedHooks


class EC2ExtendedCreateInstance(BaseOperator):
    """
    Create AWS EC2 instance using boto3.

    :param subnet_id: subnet id for AWS EC2 instance
    :type instance_id: str
    :param security_group_ids: List of security group ids
    :type instance_id: List[str]
    :param image_id: EC2 image id
    :type image_id: str
    :param instance_type: EC2 Instance type
    :type instance_type: str
    :param region_name: AWS Region
    :type region_name: str
    :param key_name: EC2 Key
    :type key_name: str
    :param tags: EC2 Tags for the EC2 Instance
    :type tags: List[Dict[str, str]]
    :param iam_instance_profile: IAM Profile
    :type iam_instance_profile: List[Dict[str, str]], Default is None
    :param user_data: EC2 User Data
    :type user_data: str
    :param min_count: Number of EC2 Instances
    :type min_count: Optional[int] = 1
    :param max_count: Number of EC2 Instances
    :type max_count: Optional[int] = 1
    :return: Instance object
    :rtype: ec2.Instance
    """
    template_fields = ["subnet_id", "security_group_ids", "image_id", "instance_type", "key_name", "region_name"]
    ui_color = "#ace1af"
    ui_fgcolor = "#00563f"

    @apply_defaults
    def __init__(self, subnet_id: str, security_group_ids: List[str], image_id: str, instance_type: str, key_name: str,
                 tags: List[Dict[str, str]], min_count: Optional[int] = 1, max_count: Optional[int] = 1,
                 aws_conn_id: str = "aws_default", region_name: Optional[str] = None, check_interval: float = 15,
                 **kwargs):
        super(EC2ExtendedCreateInstance, self).__init__(**kwargs)
        self.subnet_id = subnet_id
        self.security_group_ids = security_group_ids
        self.image_id = image_id
        self.instance_type = instance_type
        self.security_group_ids = security_group_ids
        self.key_name = key_name
        self.tags = tags
        self.max_count = max_count
        self.min_count = min_count
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.check_interval = check_interval

    def execute(self, context: 'Context'):
        EC2Instance = EC2ExtendedHooks(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name
        )
        self.log.info("Creating EC2 instance with image id %s on subnet %s", self.image_id, self.subnet_id)
        instance = EC2Instance.create_instance(
            subnet_id=self.subnet_id,
            security_group_ids=self.security_group_ids,
            image_id=self.image_id,
            instance_type=self.instance_type,
            key_name=self.key_name,
            region_name=self.region_name,
            tags=self.tags,
            min_count=self.min_count,
            max_count=self.max_count
        )

        ec2_hook = EC2Hook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)
        ec2_hook.wait_for_state(
            instance_id=instance.id,
            target_state="running",
            check_interval=self.check_interval,
        )
        return instance.id, instance.private_ip_address
