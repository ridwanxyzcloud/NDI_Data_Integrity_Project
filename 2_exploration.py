# data investigation
## count data and report
 
ndi_data.count()



# cleaning the data 
## drop duplicates in an optimised way 

ndi_data.count() - ndi_data.dropDuplicates().count() 


## check for null value using expression
ndi_data.select('ssn')\
        .filter(expr("ssn is null"))\
        .show()

ndi_data.select('*')\
    .filter(expr("salary is null OR ssn is null"))\
        .show()

### efficient and optimized null check
null_check = ' OR ' .join(f'{col} is null' for col in ndi_data.columns)

ndi_data.select('*').filter(expr(null_check)).show()

## checking summary statistics for salary
ndi_data.select('salary').summary().show()

# min salary is -2.
## investigate it 
ndi_data.filter(expr('salary < 0')).show()

## checking for unique values
unique_gender = ndi_data.select('gender')\
                        .distinct()\
                        .collect() # collect from every single nodes
# unique_gender.show()
display(unique_gender)