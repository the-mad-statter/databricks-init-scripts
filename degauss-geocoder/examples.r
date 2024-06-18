# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Shell

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Geocode Single Address

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC ruby /app/geocode.rb "3333 Burnet Ave Cincinnati OH 45229"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Use entrypoint.R

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC cd /Workspace/Users/schuelke@wustl.edu
# MAGIC wget https://raw.githubusercontent.com/degauss-org/geocoder/master/test/my_address_file.csv
# MAGIC Rscript /app/entrypoint.R my_address_file.csv 0.5

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # R

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Read and Write CSV

# COMMAND ----------


setwd("/Workspace/Users/schuelke@wustl.edu")

csv_in <- "my_address_file_in.csv"
csv_out <- "my_address_file_out.csv"

download.file("https://raw.githubusercontent.com/degauss-org/geocoder/master/test/my_address_file.csv", csv_in)

readr::read_csv(csv_in) |>
  purrr::pmap_dfr(\(id, address, ...) {
    result <- system2("ruby", args = c("/app/geocode.rb", shQuote(address)), stdout = TRUE, stderr = FALSE) |>
      jsonlite::fromJSON()

    tibble::tibble(id = id, address = address) |>
      dplyr::bind_cols(result)
  }) |>
  readr::write_csv(csv_out)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read and Write Data Lake

# COMMAND ----------


# download an example csv input file
download.file(
  "https://raw.githubusercontent.com/degauss-org/geocoder/master/test/my_address_file.csv", 
  "/Workspace/Users/schuelke@wustl.edu/my_address_file.csv"
)

# read the example csv input file and write to lake
SparkR::read.df("file:/Workspace/Users/schuelke@wustl.edu/my_address_file.csv", "csv", header = "true") |>
SparkR::saveAsTable("sandbox.wilcox_lab.degauss_geocoder_my_address_file", "delta", "overwrite")

# create user defined function (udf) version of geocode() so that it can be applied to a pyspark dataframe
geocode <- function(df) {
  df |> 
    purrr::pmap(\(id, address) {
      result <- system2("ruby", args = c("/app/geocode.rb", shQuote(address)), stdout = TRUE, stderr = FALSE) |>
      jsonlite::fromJSON()
    
      tibble::tibble(id, address) |>
        dplyr::bind_cols(result)
    }) |>
  purrr::list_rbind()
}

# resulting schema after dapply() geocode()
schema <- SparkR::structType(
  SparkR::structField("id", "string"), 
  SparkR::structField("address", "string"),
  SparkR::structField("street", "string"),
  SparkR::structField("zip", "string"),
  SparkR::structField("city", "string"),
  SparkR::structField("state", "string"),
  SparkR::structField("lat", "double"),
  SparkR::structField("lon", "double"),
  SparkR::structField("fips_county", "string"),
  SparkR::structField("score", "double"),
  SparkR::structField("prenum", "string"),
  SparkR::structField("number", "string"),
  SparkR::structField("precision", "string")
)

# process the data without ever leaving spark
SparkR::sql("SELECT * FROM sandbox.wilcox_lab.degauss_geocoder_my_address_file;") |>
SparkR::dapply(geocode, schema) |>
SparkR::saveAsTable("sandbox.wilcox_lab.degauss_geocoder_my_address_file_out", "delta", "overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Python

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Read and Write CSV

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC import urllib.request
# MAGIC import json
# MAGIC import subprocess
# MAGIC import pandas as pd
# MAGIC
# MAGIC csv_in = "/Workspace/Users/schuelke@wustl.edu/my_address_file.csv"
# MAGIC csv_out = "/Workspace/Users/schuelke@wustl.edu/my_address_file_out.csv"
# MAGIC
# MAGIC # download an example csv input file
# MAGIC urllib.request.urlretrieve(
# MAGIC     "https://raw.githubusercontent.com/degauss-org/geocoder/master/test/my_address_file.csv", 
# MAGIC     csv_in
# MAGIC )
# MAGIC
# MAGIC # read the example csv input file
# MAGIC df = pd.read_csv(csv_in)
# MAGIC
# MAGIC # define a geocoding function
# MAGIC def geocode(address):
# MAGIC     """
# MAGIC     Geocode an address using DeGAUSS Geocoder.
# MAGIC
# MAGIC     Parameters
# MAGIC     ----------
# MAGIC     address : string
# MAGIC         The address to code
# MAGIC     
# MAGIC     Returns
# MAGIC     -------
# MAGIC     a list [] of zero or more dictionaries {x:y}
# MAGIC         geocode information
# MAGIC     """
# MAGIC
# MAGIC     try:
# MAGIC         result = json.loads(subprocess.run(["ruby", "/app/geocode.rb", address], capture_output=True).stdout.decode())
# MAGIC     except Exception as e:
# MAGIC         result = json.loads('[{"error":"' + str(e) + '"}]')
# MAGIC
# MAGIC     return(result)
# MAGIC
# MAGIC # apply the geocoding function to the address column
# MAGIC df['json'] = df['address'].apply(geocode)
# MAGIC
# MAGIC # expand data longer (some addresses will return multiple geocode results)
# MAGIC df = df.drop(columns = ['json']).join(df['json'].explode().to_frame())
# MAGIC
# MAGIC # expand data wider
# MAGIC df = df.drop(columns = ['json']).join(pd.json_normalize(df['json']))
# MAGIC
# MAGIC # write data out
# MAGIC df.to_csv(csv_out)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Read and Write Data Lake

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC import urllib.request
# MAGIC import json
# MAGIC import subprocess
# MAGIC from pyspark.sql.functions import col, udf, explode
# MAGIC from pyspark.sql.types import ArrayType, MapType, StringType
# MAGIC
# MAGIC # download an example csv input file
# MAGIC urllib.request.urlretrieve(
# MAGIC     "https://raw.githubusercontent.com/degauss-org/geocoder/master/test/my_address_file.csv", 
# MAGIC     "/Workspace/Users/schuelke@wustl.edu/my_address_file.csv"
# MAGIC )
# MAGIC
# MAGIC # read the example csv input file and write to lake
# MAGIC (
# MAGIC     spark
# MAGIC     .read
# MAGIC     .format("csv")
# MAGIC     .option("header", True)
# MAGIC     .load("file:/Workspace/Users/schuelke@wustl.edu/my_address_file.csv") # path must be absolute
# MAGIC     .writeTo("sandbox.wilcox_lab.degauss_geocoder_my_address_file")
# MAGIC     .createOrReplace()
# MAGIC )
# MAGIC
# MAGIC # define a geocoding function
# MAGIC def geocode(address):
# MAGIC     """
# MAGIC     Geocode an address using DeGAUSS Geocoder.
# MAGIC
# MAGIC     Parameters
# MAGIC     ----------
# MAGIC     address : string
# MAGIC         The address to code
# MAGIC     
# MAGIC     Returns
# MAGIC     -------
# MAGIC     a list [] of zero or more dictionaries {x:y}
# MAGIC         geocode information
# MAGIC     """
# MAGIC
# MAGIC     try:
# MAGIC         result = json.loads(subprocess.run(["ruby", "/app/geocode.rb", address], capture_output=True).stdout.decode())
# MAGIC     except Exception as e:
# MAGIC         result = json.loads('[{"error":"' + str(e) + '"}]')
# MAGIC
# MAGIC     return(result)
# MAGIC
# MAGIC # create user defined function (udf) version of geocode() so that it can be applied to a pyspark dataframe
# MAGIC geocodeUDF = udf(lambda x:geocode(x), ArrayType(MapType(StringType(), StringType())))
# MAGIC
# MAGIC # process the data without ever leaving spark
# MAGIC (
# MAGIC     spark
# MAGIC     .sql("SELECT * FROM sandbox.wilcox_lab.degauss_geocoder_my_address_file")
# MAGIC     .withColumn("geocode_results", geocodeUDF(col("address")))
# MAGIC     # expand data longer (some addresses will return multiple geocode results)
# MAGIC     .withColumn("geocode_results", explode(col("geocode_results")))
# MAGIC     # expand data wider
# MAGIC     .withColumn("street", col("geocode_results")["street"])
# MAGIC     .withColumn("zip", col("geocode_results")["zip"])
# MAGIC     .withColumn("city", col("geocode_results")["city"])
# MAGIC     .withColumn("state", col("geocode_results")["state"])
# MAGIC     .withColumn("lat", col("geocode_results")["lat"])
# MAGIC     .withColumn("lon", col("geocode_results")["lon"])
# MAGIC     .withColumn("fips_county", col("geocode_results")["fips_county"])
# MAGIC     .withColumn("score", col("geocode_results")["score"])
# MAGIC     .withColumn("prenum", col("geocode_results")["prenum"])
# MAGIC     .withColumn("number", col("geocode_results")["number"])
# MAGIC     .withColumn("precision", col("geocode_results")["precision"])
# MAGIC     .withColumn("error", col("geocode_results")["error"])
# MAGIC     # geocode_results are no longer needed
# MAGIC     .drop("geocode_results")
# MAGIC     .writeTo("sandbox.wilcox_lab.degauss_geocoder_my_address_file_out")
# MAGIC     .createOrReplace()
# MAGIC )
