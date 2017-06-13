[![Build Status](https://travis-ci.org/mattkohl/Rollout.svg?branch=master)](https://travis-ci.org/mattkohl/Rollout) 
[![Coverage Status](https://coveralls.io/repos/github/mattkohl/Rollout/badge.svg?branch=master)](https://coveralls.io/github/mattkohl/Rollout?branch=master)

# Rollout

A PySpark script that builds a static site from an RDF source

### Install

Clone the project with this command:

```bash
$ git clone https://github.com/mattkohl/Rollout.git
```
    
If you don't have Spark, download it [here](http://spark.apache.org/downloads.html), or run `install-spark.sh` for a tmp installation:

```bash
$ ./install-spark.sh
```

Create a [virtual environment](https://pypi.python.org/pypi/virtualenv), activate it, and install the dependencies:

```bash
$ virtualenv -p python3 ~/.virtualenvs/Rollout
$ source ~/.virtualenvs/Rollout/bin/activate
(Rollout)$ pip install -r requirements.txt
```

### Test
Set your `SPARK_HOME` environment variable like so:
```bash
(Rollout)$ export SPARK_HOME=~/your/path/to/spark
```
Then execute this command:
```bash
(Rollout)$ pytest -q tests/* 
```

Or with coverage:

```bash
(Rollout)$ pytest --cov=Rollout tests/*
```

### Run 

Set Spark's Python environment variable:

```bash
(Rollout)$ export PYSPARK_PYTHON=python3
```

Navigate to `/your_spark/bin/` & execute the following:

```bash
(Rollout)$ ./spark-submit /path/to/Rollout/Rollout.py /your_source_directory/input.nq /your_output_directory/
```

### View the Results

`/your_output_directory/_index.html` should open automatically in your browser.
