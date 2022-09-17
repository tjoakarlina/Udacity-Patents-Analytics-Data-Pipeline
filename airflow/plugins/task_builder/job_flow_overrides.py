JOB_FLOW_OVERRIDES = {
    "Name": "ETL",
    "ReleaseLabel": "emr-6.5.0",
    "LogUri": "s3://aws-logs-735201909841-ap-southeast-1/elasticmapreduce/",
    "Applications": [
        {"Name": "Hadoop"},
        {"Name": "Spark"},
    ],  # We want our EMR cluster to have HDFS and Spark
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {
                        "PYSPARK_PYTHON": "/usr/bin/python3"
                    },  # by default EMR uses py2, change it to py3
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT",  # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,  # this lets us programmatically terminate the cluster
    },
    "EbsRootVolumeSize": 25,
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}


JOB_FLOW_OVERRIDES_NLP_SPARK = {
    "Name": "ETL",
    "ReleaseLabel": "emr-6.5.0",
    "LogUri": "s3://aws-logs-735201909841-ap-southeast-1/elasticmapreduce/",
    "Applications": [
        {"Name": "Hadoop"},
        {"Name": "Spark"},
    ],  # We want our EMR cluster to have HDFS and Spark
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {
                        "PYSPARK_PYTHON": "/usr/bin/python3"
                    },  # by default EMR uses py2, change it to py3
                }
            ],
        },
        {
            "Classification": "spark-defaults",
            "Properties": {
                "spark.driver.memory": "10g",
                "spark.kryoserializer.buffer.max": "2000M",
                "spark.driver.maxResultSize": "0",
                "spark.jars.packages": "com.johnsnowlabs.nlp:spark-nlp_2.12:4.1.0",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.yarn.stagingDir": "hdfs:///tmp",
                "spark.yarn.preserve.staging.files": "true",
            },
        },
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT",  # Spot instances are a "use as available" instances
                "InstanceRole": "CORE",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,  # this lets us programmatically terminate the cluster
    },
    "EbsRootVolumeSize": 25,
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    "BootstrapActions": [
        {
            "Name": "Spark NLP Bootstrap",
            "ScriptBootstrapAction": {
                "Path": "s3://patents-analytics/spark_nlp_bootstrap.sh",
                "Args": [],
            },
        }
    ],
}
