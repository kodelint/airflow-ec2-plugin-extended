from airflow.plugins_manager import AirflowPlugin
from hooks.ec2_instance_hooks import *
from operators.ec2_create_instance import *
from operators.ec2_terminate_instance import *


class EC2ExtendedPlugins(AirflowPlugin):
    """
    Class definition for Airflow EC2 Plugin
    Supports:
    - Creating EC2 Instances
    - Destroying EC2 Instances
    """
    name = 'ec2_extended_plugins'
    # List of hooks implemented under `EC2ExtendedPlugins` Class
    hooks = [EC2ExtendedHooks]
    # List of operators implemented under `EC2ExtendedPlugins` Class
    operators = [EC2ExtendedCreateInstance, EC2ExtendedTerminateInstance]
