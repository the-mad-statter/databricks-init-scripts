# Databricks notebook source
# MAGIC %md
# MAGIC # Example 1: Command Line Call for One Address

# COMMAND ----------

# MAGIC %md
# MAGIC ## Absolute

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC # absolute path to ruby script
# MAGIC ruby /root/geocoder/bin/geocode.rb "3333 Burnet Ave Cincinnati OH 45229"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alias

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC # alias for the above
# MAGIC geocode "3333 Burnet Ave Cincinnati OH 45229"

# COMMAND ----------

# MAGIC %md
# MAGIC ## entrypoint.R version

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC # DeGAUSS provides an R script file to process a .csv file.
# MAGIC # The script calls geocode.rb like this.
# MAGIC ruby /app/geocode.rb "3333 Burnet Ave Cincinnati OH 45229"

# COMMAND ----------

# MAGIC %md
# MAGIC # Example 2: R Code to Process a .csv File

# COMMAND ----------

# MAGIC %md
# MAGIC ## R code to return all results

# COMMAND ----------

# an example csv input file from the install
csv_in <- "/root/geocoder/test/my_address_file.csv"

# desired csv output location
csv_out <- "/root/geocoder/test/my_address_file_out.csv"

readr::read_csv(csv_in) |>
  purrr::pmap_dfr(\(id, address, ...) {
    result <- system2("ruby", args = c("/root/geocoder/bin/geocode.rb", shQuote(address)), stdout = TRUE, stderr = FALSE) |>
      jsonlite::fromJSON()

    tibble::tibble(id = id, address = address) |>
      dplyr::bind_cols(result)
  }) |>
  readr::write_csv(csv_out)

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC # view contents of csv output file
# MAGIC less /root/geocoder/test/my_address_file_out.csv

# COMMAND ----------

# MAGIC %md
# MAGIC ## DeGAUSS entrypoint.R script

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC # Run the entrypoint.R script against the csv input file, and write results to a corresponding csv output file named according to the input file name appended with "geocoder", "3.3.0", "score_threshold", and the value for score threshold, all deliniated by "_". The score threshold can be passed as an optional second argument to the R script and ranges between 0 (i.e., all) and 1 with 0.5 being the default. Therefore, the output file in this case will be "my_address_file_1_geocoder_3.3.0_score_threshold_0.7.csv".
# MAGIC cd /root/geocoder/test
# MAGIC Rscript /root/geocoder/entrypoint.R my_address_file.csv 0.7

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC # view contents of csv output file
# MAGIC less /root/geocoder/test/my_address_file_geocoder_3.3.0_score_threshold_0.7.csv

# COMMAND ----------

# MAGIC %md
# MAGIC # Example 3: PySpark to Read, Process, and Write from the Warehouse

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC # because python and sql can't read from ephemeral storage, copy example address file to /Volumes
# MAGIC cp /root/geocoder/test/my_address_file.csv /Volumes/sandbox/wilcox_lab/volume/tmp/my_address_file.csv

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC # read example csv input file from Volumes and write to warehouse
# MAGIC (
# MAGIC     spark
# MAGIC     .read
# MAGIC     .format("csv")
# MAGIC     .option("header", True)
# MAGIC     .load("/Volumes/sandbox/wilcox_lab/volume/tmp/my_address_file.csv")
# MAGIC     .writeTo("sandbox.wilcox_lab.degauss_geocoder_my_address_file")
# MAGIC     .createOrReplace()
# MAGIC )

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC import json
# MAGIC import subprocess
# MAGIC from pyspark.sql.functions import col, udf, explode
# MAGIC from pyspark.sql.types import ArrayType, MapType, StringType
# MAGIC
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
# MAGIC         result = json.loads(subprocess.run(["ruby", "/root/geocoder/bin/geocode.rb", address], capture_output=True).stdout.decode())
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
