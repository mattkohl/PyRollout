import sys
import os
import shutil
import argparse
import time
from Rollout import build_graph, env, write_index_html


RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


def reserialize(rdf_file):
    g = build_graph(rdf_file)
    print(len(g), "statements parsed from", rdf_file, ".")
    print("Reserializing to nquads.")
    s = g.serialize(format="nquads")
    return s


def extract_quads(lines):
    return lines.map(lambda line: line.replace("<", "").replace(">", "").split()[:-1])


def build_index(quads):
    rdftype_uri_hash_tups = quads.\
        filter(lambda quad: quad[1] == RDF_TYPE).\
        map(lambda quad: (quad[2], (quad[0], str(hash(quad[0]))))).\
        groupByKey().\
        mapValues(list)
    return rdftype_uri_hash_tups.collectAsMap()


def build_cbds(quads):
    cbds = quads.map(lambda quad: ((quad[0], str(hash(quad[0]))), (quad[1], quad[2], str(hash(quad[2]))))).\
        groupByKey().\
        mapValues(list).\
        collectAsMap()
    return cbds


def write_resource_html(subject, cbd, subjects, out_path="output"):
    (subject, uri_hash) = subject

    outfile = os.path.join(out_path, str(uri_hash) + '.html')
    template = env.get_template('resource.html')
    bnode = not subject.startswith("http")

    with open(outfile, 'w', encoding='utf-8') as fn:
        html = template.render(subjects=subjects, subject=subject, cbd=cbd, bnode=bnode)
        fn.write(html)
        fn.close()
        return outfile


if __name__ == '__main__':
    start = time.clock()
    try:
        from pyspark import SparkContext
        from pyspark import SparkConf
        print("Successfully imported Spark Modules")

    except ImportError as e:
        print("Cannot import Spark Modules", e)
        sys.exit(1)

    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=str, help="RDF input filename")
    parser.add_argument("output", type=str, help="output directory")
    args = parser.parse_args()

    source = args.input

    output_path = args.output

    if os.path.exists(output_path):
        try:
            print(output_path, 'exists. Attempting to delete.')
            shutil.rmtree(output_path)
        except:
            raise(OSError('Unable to refresh output directory.'))
    os.mkdir(output_path)

    conf = SparkConf().setMaster("local").setAppName("My App")
    sc = SparkContext(conf=conf)

    if source.endswith(".nq"):
        lines = sc.textFile(source)
    else:
        print("Found a serialization other than nquads.")
        print("Parsing & attempting to re-serialize.")
        string = reserialize(source).decode("utf-8")
        lines = sc.parallelize(string.split("\n"))

    quads = lines.\
        filter(lambda line: line.endswith(" .")).\
        map(lambda line: line.replace("<", "").replace(">", "").split()[:-1])

    index = build_index(quads)
    write_index_html(index, output_path)

    cbds = build_cbds(quads)
    subjects = [t[0] for t in cbds.keys()]
    [write_resource_html(key, cbds[key], subjects, output_path) for key in cbds]

    # write_resource_html(subject, cbd.toLocalIterator(), set(subjects.toLocalIterator()), output_path)
