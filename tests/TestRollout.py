import os
import logging
import pytest

from pyspark import SparkConf
from pyspark import SparkContext

from Rollout import RDF_TYPE, get_source_format, build_index, build_cbds, \
    fill_template, extract_triples, parse_args, prepare_output_directory, \
    write_html, get_index_filename, get_subjects


TRIPLES = [
    ('http://example.org/1234', RDF_TYPE, "http://example.org/Person"),
    ('http://example.org/2345', RDF_TYPE, "http://example.org/Person"),
]

TEST_RDF_PATH = "tests/resources/test.nt"


def test_parse_args():
    a1, a2 = parse_args(['test1.ttl', 'test1'])
    assert a1 == "test1.ttl"
    assert a2 == "test1"


def test_parse_bad_args():
    with pytest.raises(SystemExit):
        parse_args([])


def test_handle_output_path(tmpdir):
    tmpdir.mkdir("sub").join("hello.txt").write("content")
    assert len(tmpdir.listdir()) == 1
    prepare_output_directory(tmpdir.strpath)
    assert len(tmpdir.listdir()) == 0


def test_handle_bad_output_path():
    with pytest.raises(Exception):
        prepare_output_directory("/")  # illegal dir name


def test_extract_triples():
    results = extract_triples(TEST_RDF_PATH)
    assert len(results) == 229


def test_extract_triples_bad_path():
    with pytest.raises(SystemExit):
        extract_triples("bad/path/here.ttl")


def test_get_source_format():
    f1 = "test.nt"
    f2 = "test.nq"
    f3 = "test.ttl"
    f4 = "test.rdf"
    assert get_source_format(f1) == "nt"
    assert get_source_format(f2) == "nquads"
    assert get_source_format(f3) == "turtle"
    assert get_source_format(f4) == "xml"


def test_build_index(spark_context):
    triples_rdd = spark_context.parallelize(TRIPLES, 1)
    index = build_index(triples_rdd)

    assert len(index) == 1
    assert "http://example.org/Person" in index
    assert len(index["http://example.org/Person"]) == 2


def test_write_index_html(tmpdir):
    instances = {
        "www.example.org/Person": [
            ("www.example.org/1234", "8245348113301530764"),
            ("www.example.org/2345", "-6590047765216027844"),
            ("www.example.org/3456", "-4985268491244785538")
        ]
    }
    file = tmpdir.join('output')
    pass


def test_build_cbds(spark_context):
    triples_rdd = spark_context.parallelize(TRIPLES, 1)
    cbds = build_cbds(triples_rdd)

    assert len(cbds) == 2
    assert ("http://example.org/1234", "-2058394215090544516") in cbds
    assert len(cbds[("http://example.org/1234", "-2058394215090544516")]) == 1


def test_fill_template_index():
    template_file = "index.html"
    heading = "heading"
    instances = []
    params = {"heading": heading, "instances": instances}
    result = fill_template(template_file, params)

    assert heading in result


def test_fill_template_resource():
    template_file = "resource.html"
    subjects = {"http://example.org/1234"}
    subject = "http://example.org/1234"
    cbd = [
        ("www.example.org/knows", "www.example.org/2345", "-6590047765216027844"),
        ("www.example.org/knows", "www.example.org/3456", "-4985268491244785538")
    ]
    bnode = False
    result = fill_template(template_file, {"subjects": subjects, "subject": subject, "cbd": cbd, "bnode": bnode})

    assert subject in result


def test_write_html(tmpdir):
    written = write_html(os.path.join(tmpdir.strpath, "temp.html"), "index.html", {"instances": [], "heading": "heading"} )
    assert written.endswith("temp.html")


def test_get_index_filename():
    i = get_index_filename("foo")
    assert i == "file://foo/_index.html"


def test_get_subjects():
    c = {("http://example.org/1234", "-2058394215090544516"): "foo"}
    s = get_subjects(c)
    assert s == ["http://example.org/1234"]


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
