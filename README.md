# Rollout
---

A PySpark script that builds a static site from an RDF source

### Install

Clone the project

    ```git clone https://github.com/mattkohl/Rollout.git```
    
If you don't have Spark, download it [here](http://spark.apache.org/downloads.html).

### Run 

Navigate to `/your_spark/bin/` & execute the following:

    ```./spark-submit /path/to/Rollout/Rollout.py /your_source_directory/input.nq /your_output_directory/```

    * N.B. `Rollout` is written in Python 3.4, so please ensure to `export PYSPARK_PYTHON=python3` *

### View the Results

Open `/your_output_directory/_index.html` in your browser