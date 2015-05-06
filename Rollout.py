import rdflib
import os
import shutil
from jinja2 import Environment, PackageLoader


env = Environment(loader=PackageLoader('Rollout', 'templates'))


def build_graph(source_file):
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

    extensions = {
        'ttl': 'turtle',
        'nq': 'nquads',
        'rdf': 'xml'
    }

    source_extension = source_file.split('.')[-1]
    if source_extension in extensions:
        return extensions[source_extension]
    else:
        return source_extension


def build_index(graph):
    instances = dict()
    for s, p, o in graph.triples((None, rdflib.RDF.type, None)):
        uri_hash = str(hash(s))
        if o in instances:
            instances[o] += [(s, uri_hash)]
        else:
            instances[o] = [(s, uri_hash)]

    template = env.get_template('index.html')
    outfile = os.path.join('output', '_index.html')
    with open(outfile, 'w') as fn:
        fn.write(template.render(instances=instances))


def get_subjects(graph):
    return [s for s in graph.subjects(None, None)]


def build_html(graph, subjects):
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
    return [(p, o, str(hash(o)) + '.html') for s, p, o in graph.triples((resource, None, None))]


if __name__ == '__main__':
    # source = 'c:/users/kohlm/desktop/es_dict_metadata.rdf'
    source = 'c:/users/kohlm/desktop/languagehub.ttl'

    if os.path.exists('output'):
        try:
            shutil.rmtree('output')
        except:
            raise(OSError('Unable to delete output directory.'))
    os.mkdir('output')

    rdf_graph = build_graph(source)

    subject_resources = get_subjects(rdf_graph)
    build_index(rdf_graph)
    build_html(rdf_graph, subject_resources)

    print('Done!')