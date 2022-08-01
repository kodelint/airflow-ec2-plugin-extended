import logging
import boto3
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.ec2 import EC2Hook
from typing import List, Dict, Optional
from botocore.exceptions import ClientError


class EC2ExtendedHooks(EC2Hook):
    """
    Interact with AWS EC2 Service.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. see also::
        :class:`~airflow.providers.amazon.aws.hooks.EC2Hook`
    """
    def __init__(self,
                 *args,
                 **kwargs):
        super(EC2ExtendedHooks, self).__init__(resource_type="ec2", *args, **kwargs)
        self.instance = None

    # noinspection PyMethodMayBeStatic
    def create_instance(self,
                        subnet_id: str,
                        security_group_ids: List[str],
                        image_id: str,
                        instance_type: str,
                        region_name: str,
                        key_name: str,
                        tags: List[Dict[str, str]],
                        iam_instance_profile: Optional[Dict[str, str]] = None,
                        user_data: Optional[str] = "",
                        min_count: Optional[int] = 1,
                        max_count: Optional[int] = 1,
                        ):
        """
        Create EC2 Instance on given subnet.

        :param subnet_id: subnet id for AWS EC2 instance
        :type instance_id: str
        :param security_group_ids: List of security group ids
        :type instance_id: List[str]
        :param image_id: EC2 image id
        :type image_id: str
        :param instance_type: EC2 Instance type
        :type instance_type: str
        param region_name: AWS Region
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
        if iam_instance_profile is None:
            iam_instance_profile = {}
        if subnet_id is None:
            raise AirflowException("Required parameter missing %s", subnet_id)
        try:
            logging.info("Will bring the bridge on Subnet: %s", subnet_id)
            ec2 = boto3.resource('ec2', region_name=region_name)
            instance = ec2.create_instances(
                ImageId=image_id,
                InstanceType=instance_type,
                KeyName=key_name,
                SubnetId=subnet_id,
                MinCount=min_count,
                MaxCount=max_count,
                UserData=user_data,
                IamInstanceProfile=iam_instance_profile,
                TagSpecifications=[
                    {
                        'ResourceType': 'instance',
                        'Tags': tags
                    }
                ],
                SecurityGroupIds=security_group_ids)[0]
            logging.info("Created instance [Instance iD = %s] and [IP Address = %s]", instance.id,
                         instance.private_ip_address)
        except ClientError:
            logging.exception("Couldn't create instance with image %s, on subnet %s.", image_id, subnet_id)
            raise
        else:
            return instance

    # noinspection PyMethodMayBeStatic
    def terminate_instance(self, instance_id: str, region_name: str):
        """
        Terminate EC2 instance filtered by id.

        :param instance_id: id of the AWS EC2 instance
        :type instance_id: str
        :param region_name: name of AWS Region
        :type region_name: str
        return: None
        """
        instance_id = instance_id
        region_name = region_name
        if instance_id is None:
            raise AirflowException("instance id is required for termination, no instance id provided")
        try:
            ec2 = boto3.resource('ec2', region_name=region_name)
            logging.info("Terminating %s instance", instance_id)
            instance = ec2.Instance(instance_id)
            instance.terminate()
        except ClientError:
            logging.exception("Couldn't Terminating instance %s", instance_id)
            raise
