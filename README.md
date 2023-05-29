# Bugs
1. Using a schema to cast the userId causes it to become Nan
2. Using a schema to cast the registration causes it to become Nan
3. `withColumn` doesn't update the data type in the schema if the new column has the same name as the old column
4. Don't use Pandas 2.0 There are breaking changes with converting to timestamps, and with UDFs
5. There's also an issue with Arrow and UDFs. This setting can fix the problem `spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", "false")`
6. There's another problem with UDFs. I don't why. `TypeError: 'JavaPackage' object is not callable`