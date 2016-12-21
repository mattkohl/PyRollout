import rdflib
import os
import shutil
import argparse
import time
import concurrent.futures
from jinja2 import Environment, PackageLoader


env = Environment(loader=PackageLoader('Rollout', 'templates'))


def build_graph(source_file):
    """
    :param source_file: RDF input filename, provided as a command line argument
    :return: an RDFLib graph in which source_file is parsed.
    """
    g = rdflib.ConjunctiveGraph()
    source_format = get_source_format(source_file)

    try:
        g.parse(source_file, format=source_format, encoding='utf-8')
    except IOError as e:
        print('Cannot parse', source_file, e)
    else:
        num_triples = len(g)
        print(source_file, 'parsed.')
        print('Found', num_triples, 'statements.')
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
    return instances


def write_index_html(instances, out_path='output'):
    """
    :param instances: a dict in which the keys are RDF.types in the source graph,
    and the values are instance uris of those types

    Populate html template with instances & write to file '_index.html'.
    """
    template = env.get_template('index.html')
    outfile = os.path.join(out_path, '_index.html')
    with open(outfile, 'w') as fn:
        fn.write(template.render(instances=instances))


def get_subjects(graph):
    """
    :param graph: source content in RDFLib graph
    :return: a list of subjects from the graph.
    """
    return [s for s in graph.subjects(None, None)]


def write_resource_html(graph, subject, subjects, out_path='output'):
    """
    :param graph: source content in RDFLib graph
    :param subject: resource to be described
    :param subjects: a list of subjects from the graph
    :param out_path: output path

    Populate html template with a concise bounded description of each subject
    & write to file, the name of which is a hash on subject.
    """
    uri_hash = hash(subject)
    outfile = os.path.join(out_path, str(uri_hash) + '.html')
    cbd = get_concise_bounded_description(graph, subject)
    template = env.get_template('resource.html')
    bnode = type(subject) is rdflib.term.BNode

    with open(outfile, 'w', encoding='utf-8') as fn:
        html = template.render(subjects=subjects, subject=subject, cbd=cbd, bnode=bnode)
        fn.write(html)
        fn.close()
        return outfile


def parallel_write_resource_html(graph, subjects, out_path='output'):
    """
    :param graph: source content in RDFLib graph
    :param subjects: a list of subjects from the graph
    :param out_path: output path

    Iterates over list of subjects and assigns the querying / html-writing to one of a number of threads.
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        future_to_html = {executor.submit(write_resource_html, graph, subject, subjects, out_path): subject for subject in subjects}
        for future in concurrent.futures.as_completed(future_to_html):
            html = future_to_html[future]
            try:
                data = future.result()
            except Exception as e:
                print('%r generated an exception: %s' % (html, e))
            else:
                print(html, "written.")


def get_concise_bounded_description(graph, resource):
    """
    :param graph: source content in RDFLib graph
    :param resource: the URI of a resource in the graph
    :return: a list of tuples, each of which contains uris of the predicate & object from each triple
    in which resource is the subject, as well as a hash of the object.
    """
    return [(p, o, str(hash(o))) for s, p, o in graph.triples((resource, None, None))]


if __name__ == '__main__':
    start = time.clock()
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

    rdf_graph = build_graph(source)

    subject_resources = list(set(get_subjects(rdf_graph)))
    print("Found", len(subject_resources), "subjects.")

    index = build_index(rdf_graph)
    print("Index built.")

    write_index_html(index, output_path)
    print("Index written to html.")

    parallel_write_resource_html(rdf_graph, subject_resources, output_path)
    print(len(subject_resources), "resources written to html.")

    end = time.clock()
    print('Done! Rollout took {:3f} seconds'.format(end-start))