[![Build Status](https://travis-ci.org/mattkohl/Rollout.svg?branch=master)](https://travis-ci.org/mattkohl/Rollout) [![Coverage Status](https://coveralls.io/repos/github/mattkohl/Rollout/badge.svg?branch=master)](https://coveralls.io/github/mattkohl/Rollout?branch=master)

# Rollout

A PySpark script that builds a static site from an RDF source

### Install

Clone the project with this command:

```bash
git clone https://github.com/mattkohl/Rollout.git
```
    
If you don't have Spark, download it [here](http://spark.apache.org/downloads.html).

Create a [virtual environment](https://pypi.python.org/pypi/virtualenv), activate it, and install the dependencies:

```bash
virtualenv -p python3 ~/.virtualenvs/Rollout
source ~/.virtualenvs/Rollout/bin/activate
pip install -r requirements.txt
```

### Test
Set your `SPARK_HOME` environment variable like so:
```bash
export SPARK_HOME=~/your/path/to/spark
```
Then execute this command:
```bash
pytest -q TestRollout.py 
```

### Run 

Set Spark's Python environment variable:

```bash
export PYSPARK_PYTHON=python3
```

Navigate to `/your_spark/bin/` & execute the following:

```bash
./spark-submit /path/to/Rollout/Rollout.py /your_source_directory/input.nq /your_output_directory/
```

### View the Results

`/your_output_directory/_index.html` should open automatically in your browser.