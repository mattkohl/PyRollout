import rdflib
import os
import shutil
import argparse
import time
from jinja2 import Environment, PackageLoader


env = Environment(loader=PackageLoader('Rollout', 'templates'))


def build_graph(source_file):
    """
    :param source_file: RDF input filename, provided as a command line argument
    :return: an RDFLib graph in which source_file is parsed.
    """
    g = rdflib.Graph()
    source_format = get_source_format(source_file)

    try:
        g.parse(source_file, format=source_format, encoding='utf-8')
    except:
        raise IOError('Cannot parse', source_file)
    else:
        num_triples = len(g)
        print(source_file, 'parsed. Found', num_triples, 'triples')
    return g


def get_source_format(source_file):
    """
    :param source_file: RDF input filename
    :return: a string indicating the RDF serialization in source_file (based on filename extension).
    """
    return rdflib.util.guess_format(source_file)


def build_index(graph):
    """
    :param graph: source content in RDFLib graph
    :return: a dict in which the keys are RDF.types in the source graph,
    and the values are instance uris of those types.
    """
    instances = dict()
    for s, p, o in graph.triples((None, rdflib.RDF.type, None)):
        uri_hash = str(hash(s))
        if o in instances:
            instances[o] += [(s, uri_hash)]
        else:
            instances[o] = [(s, uri_hash)]
    return(instances)


def write_index_html(instances):
    """
    :param instances: a dict in which the keys are RDF.types in the source graph,
    and the values are instance uris of those types

    Populate html template with instances & write to file '_index.html'.
    """
    template = env.get_template('index.html')
    outfile = os.path.join('output', '_index.html')
    with open(outfile, 'w') as fn:
        fn.write(template.render(instances=instances))


def get_subjects(graph):
    """
    :param graph: source content in RDFLib graph
    :return: a list of subjects from the graph.
    """
    return [s for s in graph.subjects(None, None)]


def write_resource_html(graph, subjects):
    """
    :param graph: source content in RDFLib graph
    :param subjects: a list of subjects from the graph

    Populate html template with a concise bounded description of each subject
    & write to file, the name of which is a hash on subject.
    """
    for subject in subjects:
        uri_hash = hash(subject)
        outfile = os.path.join('output', str(uri_hash) + '.html')
        cbd = get_concise_bounded_description(graph, subject)
        template = env.get_template('resource.html')
        if type(subject) is rdflib.term.BNode:
            bnode = True
        else:
            bnode = False

        with open(outfile, 'w', encoding='utf-8') as fn:
            html = template.render(subjects=subjects, subject=subject, cbd=cbd, bnode=bnode)
            fn.write(html)


def get_concise_bounded_description(graph, resource):
    """
    :param graph: source content in RDFLib graph
    :param resource: the URI of a resource in the graph
    :return: a list of tuples, each of which contains uris of the predicate & object from each triple
    in which resource is the subject, as well as a hash of the object.
    """
    return [(p, o, str(hash(o)) + '.html') for s, p, o in graph.triples((resource, None, None))]


if __name__ == '__main__':
    start = time.clock()
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", type=str, help="RDF input filename", required=True)
    args = parser.parse_args()

    source = args.input

    if os.path.exists('output'):
        try:
            shutil.rmtree('output')
        except:
            raise(OSError('Unable to delete output directory.'))
    os.mkdir('output')

    rdf_graph = build_graph(source)

    subject_resources = get_subjects(rdf_graph)
    index = build_index(rdf_graph)

    write_index_html(index)
    write_resource_html(rdf_graph, subject_resources)
    end = time.clock()

    print('Done! Rollout took{:3f} seconds'.format(end-start))