import sys
import os
import shutil
import argparse
import time
import webbrowser
import rdflib
from jinja2 import Environment, PackageLoader


RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

env = Environment(loader=PackageLoader("Rollout", "templates"))


def extract_triples(source_file):
    g = rdflib.ConjunctiveGraph()
    source_format = get_source_format(source_file)
    try:
        g.parse(source_file, format=source_format)
    except IOError as e:
        print("Cannot parse", source_file, e)
        sys.exit(1)
    else:
        print(source_file, "parsed. Found", len(g), "triples.")
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
        ("www.example.org/Person": [
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


def write_index_html(instances, input_path, out_path="output"):
    """
    :param instances: a dict in which the keys are RDF.types in the source graph,
    and the values are instance uris of those types
    :param input_path: source path/filename - becomes index heading
    :param out_path: where to write output

    Populate html template with instances & write to file "_index.html".
    """
    template = env.get_template("index.html")
    outfile = os.path.join(out_path, "_index.html")
    heading = os.path.split(input_path)[-1]
    with open(outfile, "w") as fn:
        fn.write(template.render(instances=instances, heading=heading))
        fn.close()


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
    template = env.get_template("resource.html")
    bnode = not subject.startswith("http")

    with open(outfile, "w", encoding="utf-8") as fn:
        html = template.render(subjects=subjects, subject=subject, cbd=cbd, bnode=bnode)
        fn.write(html)
        fn.close()
        return outfile


if __name__ == "__main__":
    start = time.clock()
    try:
        from pyspark import SparkContext
        from pyspark import SparkConf
        print("Successfully imported Spark Modules")

    except ImportError as e:
        print("Cannot import Spark Modules", e)
        sys.exit(1)

    conf = SparkConf().setMaster("local").setAppName("Rollout")
    sc = SparkContext(conf=conf)

    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=str, help="RDF input filename")
    parser.add_argument("output", type=str, help="output directory")
    args = parser.parse_args()
    source = args.input
    output_path = args.output

    # Attempts to clean up output directory
    if os.path.exists(output_path):
        try:
            print(output_path, "exists. Attempting to delete.")
            shutil.rmtree(output_path)
        except:
            raise(OSError("Unable to refresh output directory."))
        else:
            print("Existing copy of", output_path, "removed.")
    os.mkdir(output_path)
    print(output_path, "created.")

    trips = extract_triples(source)

    print("Building index.")
    index = build_index(trips)
    write_index_html(index, source, output_path)
    print("Index written to", os.path.join(output_path, "_index.html"))

    print("Building resource pages.")
    cbds = build_cbds(trips)
    subjs = [t[0] for t in cbds.keys()]
    [write_resource_html(key, cbds[key], subjs, output_path) for key in cbds]
    print(len(subjs), "resource pages written.")

    end = time.clock()
    total_time = end - start
    m, s = divmod(total_time, 60)
    h, m = divmod(m, 60)

    print("Done! Rollout took %d:%02d:%02d to finish." % (h, m, s))

    print("To start exploring, open up", os.path.join(output_path, "_index.html"))
    webbrowser.open_new_tab("file://" + os.path.join(output_path, "_index.html"))
