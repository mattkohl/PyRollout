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
```bash
pytest -q TestRollout.py 
```

### Run 

Navigate to `/your_spark/bin/` & set Spark's Python environment variable:

```bash
export PYSPARK_PYTHON=python3
```

Then execute the following:

```bash
./spark-submit /path/to/Rollout/Rollout.py /your_source_directory/input.nq /your_output_directory/
```

### View the Results

`/your_output_directory/_index.html` should open automatically in your browser.