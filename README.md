# Rollout

A PySpark script that builds a static site from an RDF source

### Install

Clone the project with this command:

```git clone https://github.com/mattkohl/Rollout.git```
    
If you don't have Spark, download it [here](http://spark.apache.org/downloads.html).

Create a [virtual environment](https://pypi.python.org/pypi/virtualenv), activate it, and install the dependencies:

```
virtualenv -p python3 ~/.virtualenvs/Rollout
source ~/.virtualenvs/Rollout/bin/activate
pip install -r requirements.txt
```

### Run 

Navigate to `/your_spark/bin/` & execute the following:

```./spark-submit /path/to/Rollout/Rollout.py /your_source_directory/input.nq /your_output_directory/```

*N.B. `Rollout` is written in Python 3, so please ensure to `export PYSPARK_PYTHON=python3`*

### View the Results

`/your_output_directory/_index.html` should open automatically in your browser.