# Rollout

A Python 3.4 script that builds a static site from an RDF source

## Install

Clone the project

    `git clone https://github.com/mattkohl/Rollout.git`

## Run with Spark *(recommended)*

Navigate to `/your_spark/bin/` & execute the following:

    `./spark-submit /path/to/Rollout/Rollout_with_Spark.py /your_source_directory/input.nq /your_output_directory/`

## Run without Spark *(only for small RDF files)*

    `python3.4 Rollout.py /your_source_directory/input.ttl /your_output_directory/`

## View the Results

Open `/your_output_directory/_index.html` in your browser