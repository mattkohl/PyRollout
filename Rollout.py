import sys
import os
import shutil
import argparse
import logging
import webbrowser
import rdflib
from jinja2 import Environment, PackageLoader


logger = logging.getLogger("RolloutLog")
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

env = Environment(loader=PackageLoader("Rollout", "templates"))


def extract_triples(source_file):
    g = rdflib.ConjunctiveGraph()
    source_format = get_source_format(source_file)
    try:
        g.parse(source_file, format=source_format)
    except IOError as e:
        msg = "Cannot parse file {}: {}".format(source_file, e)
        logger.error(msg)
        sys.exit(1)
    else:
        msg = "{} parsed. Found {} triples.".format(source_file, len(g))
        logger.info(msg)
        return g


def parallelize_triples(g):
    return sc.parallelize([(s.toPython(), p.toPython(), o.toPython()) for s, p, o in g])


def get_source_format(source_file):
    """
    :param source_file: RDF input filename
    :return: a string indicating the RDF serialization in source_file (based on filename extension).
    """
    return rdflib.util.guess_format(source_file)


def build_index(triples):
    """
    :param triples: an RDD of tuple3s (subject URI, predicate URI, object URI)
    :return: a dictionary in which the keys are RDF types & the values are lists of instances
    of those types. Each item in the list is a tuple2 (instance URI, has of instance URI).

    For example:
    {
        "www.example.org/Person": [
            ("www.example.org/1234", "8245348113301530764"),
            ("www.example.org/2345", "-6590047765216027844"),
            ("www.example.org/3456", "-4985268491244785538")
        ]
    }
    """
    rdftype_uri_hash_tups = triples.\
        filter(lambda triple: triple[1] == RDF_TYPE).\
        map(lambda triple: (triple[2], (triple[0], str(hash(triple[0]))))).\
        groupByKey().\
        mapValues(list)
    return rdftype_uri_hash_tups.collectAsMap()


def fill_template(template_file, template_params):
    template = env.get_template(template_file)
    return template.render(**template_params)


def build_cbds(triples):
    """
    :param triples:  an RDD of tuple3s (subject URI, predicate URI, object URI)
    :return: a dictionary of concise bounded descriptions. The key is a tuple2 (subject URI, its hash)
     & the value is a list of tuple3s (predicate URI, object URI, hash of Object URI).

    For example:
    {
        ("www.example.org/1234", "8245348113301530764"): [
            ("www.example.org/knows", "www.example.org/2345", "-6590047765216027844"),
            ("www.example.org/knows", "www.example.org/3456", "-4985268491244785538")
        ]
    }
    """
    cbds = triples.map(lambda triple: ((triple[0], str(hash(triple[0]))), (triple[1], triple[2], str(hash(triple[2]))))).\
        groupByKey().\
        mapValues(list).\
        collectAsMap()
    return cbds


def write_resource_html(subject_tuple, cbd, subjects, out_path="output"):
    """
    :param subject_tuple: (uri of resource to write, a hash of that)
    :param cbd: concise bounded description of resource (See function build_cbds for more info)
    :param subjects: set of all subject uris
    :param out_path: where to write output
    :return: filename
    """
    (subject, uri_hash) = subject_tuple
    outfile = os.path.join(out_path, str(uri_hash) + ".html")
    bnode = not subject.startswith("http")
    return write_html(outfile, "resource.html", {"subjects": subjects, "subject": subject, "cbd": cbd, "bnode": bnode})


def write_index_html(instances, input_path, out_path="output"):
    """
    :param instances: a dict in which the keys are RDF.types in the source graph,
    and the values are instance uris of those types
    :param input_path: source path/filename - becomes index heading
    :param out_path: where to write output

    Populate html template with instances & write to file "_index.html".
    """
    outfile = os.path.join(out_path, "_index.html")
    heading = os.path.split(input_path)[-1]
    return write_html(outfile, "index.html", {"instances": instances, "heading": heading})


def write_html(filename, template, data_dict):
    with open(filename, "w", encoding="utf-8") as fn:
        html = fill_template(template, data_dict)
        fn.write(html)
        fn.close()
        return filename


def parse_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=str, help="RDF input filename")
    parser.add_argument("output", type=str, help="output directory")
    parsed = parser.parse_args(args)
    return parsed.input, parsed.output


def prepare_output_directory(outpath):
    if os.path.exists(outpath):
        logger.warning("{} exists. Attempting to delete.".format(outpath))
        try:
            shutil.rmtree(outpath)
        except Exception as e:
            raise("Unable to refresh output directory:", e)
        else:
            logger.info("Existing copy of {} removed.".format(outpath))
    os.mkdir(outpath)
    logger.info("{} created.".format(outpath))


def import_spark():  # pragma: no cover
    try:
        from pyspark import SparkContext
        from pyspark import SparkConf
    except ImportError as e:
        msg = "Cannot import Spark Modules: {}".format(e)
        logger.error(msg)
        sys.exit(1)
    else:
        logger.info("Successfully imported Spark Modules")
        spark_conf = SparkConf().setMaster("local").setAppName("Rollout")
        spark_context = SparkContext(conf=spark_conf)
        return spark_conf, spark_context


def get_index_filename(output_dir):
    return "file://" + os.path.join(output_dir, "_index.html")


def get_subjects(cbds):
    return [t[0] for t in cbds.keys()]


def pipeline(source, output_path):
    prepare_output_directory(output_path)

    triples_graph = extract_triples(source)
    triples_rdd = parallelize_triples(triples_graph)

    index = build_index(triples_rdd)
    write_index_html(index, source, output_path)

    resource_pages = build_cbds(triples_rdd)
    [write_resource_html(key, resource_pages[key], get_subjects(resource_pages), output_path) for key in resource_pages]

    return get_index_filename(output_path)


if __name__ == "__main__":  # pragma: no cover

    # set up Spark
    conf, sc = import_spark()

    # read in source RDF & path to write static site
    source_rdf, output_directory = parse_args(sys.argv[1:])

    done = pipeline(source_rdf, output_directory)
    webbrowser.open_new_tab(done)
