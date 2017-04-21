import findspark  # this needs to be the first import
findspark.init()

import logging
import pytest

from pyspark import SparkConf
from pyspark import SparkContext

from .Rollout import RDF_TYPE, get_source_format, build_index, build_cbds


TRIPLES = [
    ('http://example.org/1234', RDF_TYPE, "http://example.org/Person"),
    ('http://example.org/2345', RDF_TYPE, "http://example.org/Person"),
]


def test_get_source_format():
    f1 = "test.nt"
    f2 = "test.nq"
    f3 = "test.ttl"
    assert get_source_format(f1) == "nt"
    assert get_source_format(f2) == "nquads"
    assert get_source_format(f3) == "turtle"


def test_build_index(spark_context):
    triples_rdd = spark_context.parallelize(TRIPLES, 1)
    index = build_index(triples_rdd)

    assert len(index) == 1
    assert "http://example.org/Person" in index
    assert len(index["http://example.org/Person"]) == 2


def test_build_cbds(spark_context):
    triples_rdd = spark_context.parallelize(TRIPLES, 1)
    cbds = build_cbds(triples_rdd)

    assert len(cbds) == 2
    assert ("http://example.org/1234", "-2058394215090544516") in cbds
    assert len(cbds[("http://example.org/1234", "-2058394215090544516")]) == 1


def quiet_py4j():
    """ turn down spark logging for the test context """
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_context(request):
    """ fixture for creating a spark context
    Args:
        request: pytest.FixtureRequest object
    """
    conf = (SparkConf().setMaster("local[2]").setAppName("pytest-pyspark-local-testing"))
    sc = SparkContext(conf=conf)
    request.addfinalizer(lambda: sc.stop())

    quiet_py4j()
    return sc