# read dataset
ndi_data = spark.read.format("parquet")\
                .option('mergeSchema', True)\
                .load("/databricks-datasets/learning-spark-v2/people/people-10m.parquet")