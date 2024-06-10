# Databricks notebook source
# MAGIC %md
# MAGIC # Example 1: Command Line Call for One Address

# COMMAND ----------

# MAGIC %md
# MAGIC ## Absolute

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC ruby /root/geocoder/bin/geocode.rb "3333 Burnet Ave Cincinnati OH 45229"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alias

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC # schuelke created alias
# MAGIC geocode "3333 Burnet Ave Cincinnati OH 45229"

# COMMAND ----------

# MAGIC %md
# MAGIC ## entrypoint.R version

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC ruby /app/geocode.rb "3333 Burnet Ave Cincinnati OH 45229"

# COMMAND ----------

# MAGIC %md
# MAGIC # Example 2: R Code to Process a .csv File

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simple

# COMMAND ----------

csv_in <- "/root/geocoder/test/my_address_file.csv"
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
# MAGIC less /root/geocoder/test/my_address_file_out.csv

# COMMAND ----------

# MAGIC %md
# MAGIC ## entrypoint.R

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC # make a copy of input file to process because corresponding output file already exists
# MAGIC cd /root/geocoder/test
# MAGIC cp my_address_file.csv my_address_file_1.csv
# MAGIC ls .

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC cd /root/geocoder/test
# MAGIC Rscript /root/geocoder/entrypoint.R my_address_file_1.csv all   

# COMMAND ----------

# MAGIC %sh
# MAGIC less /root/geocoder/test/my_address_file_1_geocoder_3.3.0_score_threshold_all.csv
