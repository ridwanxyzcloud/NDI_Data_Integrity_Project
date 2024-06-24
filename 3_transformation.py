
clean_df2 = ndi_data.select('*')\
                    .filter(expr('salary >= 0'))\
                    .withColumn('birthDate', to_date('birthdate'))\
                    .withColumn('ssn',concat(substring('ssn' 1, 3), litt(-xx-xxxx)))\
                    .withColumn('age', datediff(current_date(), col('birthDate'))/365)\
                    .withColumn('age', round('age').cast('int'))