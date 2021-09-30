from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("udf1").getOrCreate()

# create python function:
def email(name1,name2):
    return name1.lower() + "." + name2.lower() + ".@company.com"

def firstName(name):
    return name.lower()

def lastName(name):
    return name.upper()

# register udf for DataFrame use
##for Pyspark 
email_udf = udf(email,StringType())
first_name_udf = udf(firstName,StringType())
last_name_udf = udf(lastName,StringType())
##for sql
spark.udf.register("email_regUDF",email,StringType())
spark.udf.register("first_name_regUDF",firstName,StringType())
spark.udf.register("last_name_regUDF",lastName,StringType())

if __name__ == "__main__":
    
    # extraction
    emp_pyspark = spark.read.format("csv").options(header=True).load("file:///home/tamaghna/files/employees_truc")
    emp_sql = spark.read.format("csv").options(header=True).load("file:///home/tamaghna/files/employees_truc")
        

    # transformation:
    emp_pyspark.withColumn("email",email_udf(emp_pyspark.first_name,emp_pyspark.last_name))\
        . withColumn("first_name",first_name_udf(emp_pyspark.first_name))\
        .withColumn("last_name",last_name_udf(emp_pyspark.last_name))
    
    emp_sql.createOrReplaceTempView("emp_table")
    emp_sql = spark.sql("\
                            select\
                                first_name_regUDF(first_name) as first_name,\
                                last_name_regUDF(last_name) as last_name,\
                                email_regUDF(first_name, last_name) as email,\
                                phone_number,\
                                hire_date\
                                from emp_table"
                     )

    # loading
    emp_pyspark.write.format("csv").options(header=True)\
        .mode("overwrite")\
        .save("file:///home/tamaghna/files/employees_transformed_pyspark")
    emp_sql.write.format("csv").options(header=True)\
        .mode("overwrite")\
        .save("file:///home/tamaghna/files/employees_transformed_sql")

    print("File written succesfully")
