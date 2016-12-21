# Rollout
A Python 3.4 script that builds a static site from an RDF source

## To Install
- Clone the project

    `git clone https://github.com/mattkohl/Rollout.git`

## To Run without PySpark
- Execute the following (N.B. two positional arguments: 1. input RDF filename, 2. output directory)

    `python3.4 Rollout.py /my_source_directory/input.ttl /my_output_directory/`

## To Run with PySpark
- Navigate to /your_spark/bin/ & execute the following:

    `./spark-submit Rollout_with_Spark.py /my_source_directory/input.nq /my_output_directory/`

## To View the results
- Open `/my_output_directory/_index.html` in your browser