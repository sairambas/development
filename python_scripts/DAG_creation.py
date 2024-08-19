from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.contrib.hooks.databricks_hook import DatabricksHook
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.models import BaseOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime, timedelta
from airflow.models import Variable
import os
import pendulum

dag_name= os.path.basename(__file__)[:-3]

email_list=''
creater = ""
interface = ""

databricks_config_loads = Variable.get("<variable_name>", deserialize_json=True)

coma_sep = "', '"

########################  default args and interval details ###################
local_tz = pendulum.timezone('EST5EDT')
default_args = {
	'owner': '<>',
	'depends_on_past': False,
	'email': f"{','.join(databricks_config_loads['email']['email_list'])}",
	'start_date': datetime(2020, 8, 31, tzinfo = local_tz),
	'email_on_failure': databricks_config_loads['email']['email_on_failure'],
	'email_on_retry': databricks_config_loads['email']['email_on_retry'],
# 	'on_failure_callback': notify_email,
	'retries': 0
}

dag = DAG(
    dag_name,
    default_args=default_args,
	#schedule_interval = '*/30 * * * 0-6',
    schedule_interval = '*/30 8-20 * * 0-6',
    catchup = False,
    max_active_runs = 1
)




library_variable = Variable.get("<>",deserialize_json=True)
library_variable["libraries"].append({"jar":"dbfs:/FileStore/jar/commons_collections4_4_4.jar"})
library_variable["libraries"].append({"jar":"dbfs:/FileStore/jar/xmlbeans_3_1_0.jar"})
library_variable["libraries"].append({"jar":"dbfs:/FileStore/jar/poi_ooxml_4_1_2.jar"})
library_variable["libraries"].append({"jar":"dbfs:/FileStore/jar/poi_ooxml_schemas_4_1_2.jar"})

default_libraries = library_variable["libraries"]


init_scripts = Variable.get("<>", deserialize_json=True)
init_lists = init_scripts["init_scripts"]

# The instance_pool_id is different for dev, test and prod databricks. Pool name: MFBEUBUYDBPOOL002
cluster_config_cluster_pool = {
    "cluster_size": "small",
    "instance_pool_id" : "<>",
    "init_scripts": [],
    "custom_tags": {
        "creater": creater,
        "interface": interface
    },
    "libraries": default_libraries
}

cluster_config_single_node = {
    "cluster_size": "single_node",
    "init_scripts": [],
    "custom_tags": {
        "creater": creater,
        "interface": interface
    },
    "libraries": default_libraries
}
cluster_config_small = {
    "cluster_size": "small",	
    "init_scripts": init_lists,	
    "custom_tags": {
        "creater": creater,
        "interface": interface
    },
	"libraries": default_libraries   
}

cluster_config_medium = {
    "cluster_size": "medium",
    "init_scripts": init_lists,
    "custom_tags": {
        "creater": creater,
        "interface": interface
    },
    "libraries": default_libraries 
}

cluster_config_large = {
    "cluster_size": "large",
    "init_scripts": init_lists,
    "custom_tags": {
        "creater": creater,
        "interface": interface
    },
    "libraries": default_libraries
}

########################  get_task function to get the cluster details ###################

def get_task(databricks_config_loads: dict, cluster_config: dict, notebook_path: str, task_id: str, base_parameters: dict = {}, dag_name: str = dag_name, dag_config_variable: str = "edp_custom_dag_config") -> dict:

    if not cluster_config.get('custom_tags'):
        raise Exception("'custom_tags' is mandatory")

    def get_new_cluster(databricks_config: dict, creater: str, interface: str, cluster_size: str = "small", init_scripts: list = []):
        cluster_size = cluster_size.lower()
        new_cluster = {
            #"autoscale": databricks_config['cluster'][cluster_size]['autoscale'],
            "spark_version": databricks_config['spark_version'],
            # spark version change specifically added for sim pipeline modernization E2E
            "spark_conf": databricks_config['cluster'][cluster_size]['spark_conf'],
            #"node_type_id": databricks_config['cluster'][cluster_size]['worker_type'],
            #"driver_node_type_id": databricks_config['cluster'][cluster_size]['driver_type'],
            "ssh_public_keys": [],
"custom_tags": {
                **custom_tags, **databricks_config['custom_tags']
            },
            "spark_env_vars": databricks_config['spark_env_vars'],
            "enable_elastic_disk": databricks_config['cluster'][cluster_size]['enable_elastic_disk'],
            "init_scripts": init_scripts
        }
        if("autoscale" in databricks_config['cluster'][cluster_size]):
            new_cluster["autoscale"] = databricks_config['cluster'][cluster_size]['autoscale']
        
        if("worker_type" in databricks_config['cluster'][cluster_size]):
            new_cluster["node_type_id"] = databricks_config['cluster'][cluster_size]['worker_type']
            
        if("driver_type" in databricks_config['cluster'][cluster_size]):
            new_cluster["driver_node_type_id"] = databricks_config['cluster'][cluster_size]['driver_type']    

        if(cluster_size == "single_node"):
            new_cluster["node_type_id"] = databricks_config['cluster'][cluster_size]['node_type']
            new_cluster["num_workers"] = 'fixed'

        if("instance_pool_id" in cluster_config):
            new_cluster["instance_pool_id"] = cluster_config["instance_pool_id"]
            # Remove node_type_id, driver_node_type_id, enable_elastic_disk as not required when pool id is specified 
            new_cluster.pop("node_type_id", None)
            new_cluster.pop("driver_node_type_id", None)
            new_cluster.pop("enable_elastic_disk", None)
        
        return new_cluster
    custom_tags = {
        "creater": cluster_config['custom_tags'].get('creater', default_args['owner']),
        "interface": cluster_config['custom_tags'].get('interface', dag_name),
        **cluster_config['custom_tags']
    }
    default_task_json = {
        'new_cluster': get_new_cluster(
            databricks_config = databricks_config_loads,
            creater = cluster_config['custom_tags'].get('creater', default_args['owner']),
            interface = cluster_config['custom_tags'].get('interface', dag_name),
            cluster_size = cluster_config.get("cluster_size", "small"),
            init_scripts = cluster_config.get("init_scripts", [])
        ),
        'notebook_task': {
            'notebook_path': notebook_path,
            "base_parameters": base_parameters
        },
        "libraries": cluster_config.get("libraries", [])
    }

    custom_dag_config = Variable.get(dag_config_variable, deserialize_json=True)
    task_JSON = (
        custom_dag_config[dag_name][task_id]
        if custom_dag_config.get(dag_name) and custom_dag_config[dag_name].get(task_id)
        else default_task_json
    )
    return task_JSON
 
########################  get_task function end ###################
    




########################  sim_dts_receipt_task ########################
ebs_intellaflo_exception_params = get_task(
	databricks_config_loads = databricks_config_loads,
    cluster_config = cluster_config_cluster_pool,
    #notebook_path = '/ORMS_DEV/pipelines/outbound/EBS/Exception_flat_file_30mins',
    notebook_path="<>",
    task_id = '<>',
    dag_name = dag_name,
    dag_config_variable = "rms_custom_dag_config"
	)
	
ebs_intellaflo_exception_task = DatabricksSubmitRunOperator(
	task_id='ebs_intellaflo_exception_task',
	dag=dag,
	databricks_conn_id='databricks_connection_edp',
	json=ebs_intellaflo_exception_params,
	timeout_seconds= 18000)


    
ebs_intellaflo_exception_task
