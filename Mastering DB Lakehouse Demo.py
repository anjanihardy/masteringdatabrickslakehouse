# Databricks notebook source
# MAGIC %md
# MAGIC This code is used for examples related to the Mastering Databricks lake house platform. To use it, you need to have a table schema and sample data, which are provided separately. You can set up the sample data on an Azure SQL instance that Databricks can access, either on a public or private IP or domain. You also need to create a storage account, preferably ADLS Gen2, to store the output data. Finally, you need to change the connection parameters accordingly to make this example work.

# COMMAND ----------

# MAGIC %scala
# MAGIC //Check if SQL server JDBC driver is available
# MAGIC Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

# COMMAND ----------

# MAGIC %scala
# MAGIC //Setup database connection string
# MAGIC val jdbcHostname = "your SQL Server Database IP or Host Name"
# MAGIC val jdbcPort = 1433
# MAGIC val jdbcDatabase = "Database Name"
# MAGIC // Create the JDBC URL without passing in the user and password parameters.
# MAGIC val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"
# MAGIC // Create a Properties() object to hold the parameters.
# MAGIC import java.util.Properties
# MAGIC val connectionProperties = new Properties()
# MAGIC connectionProperties.put("user", s"datbaseuserid")
# MAGIC connectionProperties.put("password", s"databsepassword")

# COMMAND ----------

# MAGIC %scala
# MAGIC //Set the connection properties
# MAGIC val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# MAGIC connectionProperties.setProperty("Driver", driverClass)

# COMMAND ----------

# MAGIC %scala
# MAGIC //Load the exam_data in the Exam_Data_From_SQLServer_DB variable
# MAGIC val ExamdataFromSQLDB = spark.read.jdbc(jdbcUrl, "oltpstore", connectionProperties)

# COMMAND ----------

# MAGIC %scala
# MAGIC //Convert the Exam_Data_From_SQLServer_DB to DataFrames for processing
# MAGIC val ExamDataInDF = ExamdataFromSQLDB.toDF()

# COMMAND ----------

# MAGIC %scala
# MAGIC // Apply filter to select exam results from all the courses
# MAGIC val FilterdExamResultsInDF = ExamDataInDF.filter($"jsonobjecttype" === "CertificateRequest").select($"jsondata")
# MAGIC FilterdExamResultsInDF.createOrReplaceTempView("resultjson")
# MAGIC display(FilterdExamResultsInDF)

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.hive.HiveContext
# MAGIC val sqlContext = new HiveContext(sc)
# MAGIC val resultjsonfilterd = sqlContext.sql("SELECT from_json(jsondata,'EventID string,OrganizationGuid string,TeamID string, UserId string,emailId string,Marks int,TotalMarks int,completiondate Date,passstate string') FROM resultjson") 
# MAGIC val resultstore = resultjsonfilterd.select("from_json(jsondata).*")
# MAGIC display(resultstore)
# MAGIC resultstore.createOrReplaceTempView("resultstore")

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.hive.HiveContext
# MAGIC // sc - existing spark context
# MAGIC val sqlContext = new HiveContext(sc)
# MAGIC val dfiltered = sqlContext.sql("select from_json(jsondata,'UserId string,emailId string,Marks int,TotalMarks int,completiondate string,passstate int') FROM oltpstore") 
# MAGIC //select(explode($"employees"))
# MAGIC //df.select(col("name.*")
# MAGIC val flattenDF = dfiltered.select("from_json(jsondata).*")
# MAGIC //val df2Flatten = flattenDF.toDF("UserId","emailId","Marks","TotalMarks","completiondate","passstate")
# MAGIC   // df2Flatten.printSchema()
# MAGIC //df2Flatten.show()
# MAGIC display(flattenDF)
# MAGIC dfiltered.createOrReplaceTempView("oltpstore1")

# COMMAND ----------

adlsAccountName = "dbstodfsdfsdf"
adlsContainerName = "dbdsdssd"
adlsFolderName = "RAW"
mountPoint = "/mnt/raw"
 
# Application (Client) ID
#applicationId = dbutils.secrets.get(scope="aks001sdgs1",key="ClientId")
 applicationId = "0b57c728-198d-479857897-997699-07d10";
# Application (Client) Secret Key
#authenticationKey = dbutils.secrets.get(scope="akv-07011",key="ClientSecret")
 authenticationKey =""
# Directory (Tenant) ID
tenandId = dbutils.secrets.get(scope="aks001sdgs1",key="TenantId")
 
endpoint = "https://login.microsoftonline.com/" + tenandId + "/oauth2/token"
source = "abfss://" + adlsContainerName + "@" + adlsAccountName + ".dfs.core.windows.net/" + adlsFolderName
 
# Connecting using Service Principal secrets and OAuth
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": applicationId,
           "fs.azure.account.oauth2.client.secret": authenticationKey,
           "fs.azure.account.oauth2.client.endpoint": endpoint}
 
# Mount ADLS Storage to DBFS only if the directory is not already mounted
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  dbutils.fs.mount(
    source = source,
    mount_point = mountPoint,
    extra_configs = configs)

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.conf.set(
# MAGIC   "fs.azure.account.key.dbstoragegen2mc4u.dfs.core.windows.net", "DIt5jItsdgsdgsdgsda92q4sdgsd-jPaDerO-sdgsdgsdg5/A==")

# COMMAND ----------

spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://newconatinerdsg4dgu@dbsgtordfsdu.dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.fs.ls("abfss://dbdemo@dbstoragesdgsu.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC resultstore.printSchema()
# MAGIC 
# MAGIC resultstore.write.mode(SaveMode.Overwrite)
# MAGIC .option("header","true")
# MAGIC .csv("abfss://dbdemo@dbstoragegsdgdg.dfs.core.windows.net/RAW/resultstore.csv")
# MAGIC resultstore.write.mode(SaveMode.Overwrite)
# MAGIC .option("header","true")
# MAGIC .parquet("abfss://dbdemo@dbstoragegesddsg.dfs.core.windows.net/RAW/resultstore.parquet")

# COMMAND ----------

# MAGIC %sql drop TABLE parquetresultstore

# COMMAND ----------

# MAGIC 
# MAGIC %sql 
# MAGIC CREATE TABLE parquetresultstore
# MAGIC USING parquet
# MAGIC OPTIONS (path "abfss://dbdemo@dbstoragegedsgsg.dfs.core.windows.net/RAW/resultstore.parquet")

# COMMAND ----------

df = spark.sql('select * from  parquetresultstore') 

df.write.format("delta").mode('overwrite').option("overwriteSchema", "true").save("abfss://dbdemo@dbstoragesdgsgs.dfs.core.windows.net/RAW/parquetresultstore.delta") 

spark.sql("CREATE TABLE IF NOT EXISTS deltaresultstore USING DELTA LOCATION 'abfss://dbdemo@dbstoragesdgsdg.dfs.core.windows.net/RAW/parquetresultstore.delta'")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from deltaresultstore where passstate = "1" and userid !="null"
