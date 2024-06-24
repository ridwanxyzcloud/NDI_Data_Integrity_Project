# Configure the Redshift connection details
spark.conf.set("spark.redshift.jdbc.url", "jdbc:redshift://<redshift-host>:<redshift-port>/<database>")
spark.conf.set("spark.redshift.jdbc.user", "<redshift-username>")
spark.conf.set("spark.redshift.jdbc.password", "<redshift-password>")
spark.conf.set("spark.redshift.jdbc.driver", "com.amazon.redshift.jdbc.Driver")

# Save the clean_df DataFrame to Redshift
clean_df.write \
    .format("com.databricks.spark.redshift") \
    .option("dbtable", "<table-name>") \
    .mode("overwrite") \
    .save()


# Loading to a snowflake db


snowflake_options = {
    "sfUrl": 'izctlei-aa59947.snowflakecomputing.com',
    "sfUser":'RIDWANCLOUD',
    "sfPassword":sf_password,
    "sfDatabase":'NDI',
    "sfSchema":'STG',
    "sfWarehouse":'COMPUTE_WH',
    "sfRole":'ACCOUNTADMIN',
    "sfport":'443',
    "dbtable":'ndi_population'
}
clean_df.write.format("snowflake").options(**snowflake_options).mode("overwrite").save()
