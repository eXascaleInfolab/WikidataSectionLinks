import argparse
from collections import defaultdict
import json
import os
import time

from pyspark import SparkContext

from logger import set_logger


INSTANCE_OF = "P31"

WIKIDATA_SERVICE_PAGES = {
    "Q4167836": "Wikimedia category",
    "Q14204246": "Wikimedia project page",
    "Q13406463": "Wikimedia list article"
}

BAD_ENTITY_TYPES = {
    "Q1071027": "personal name",
    "Q101352": "family name",
    "Q202444": "given name",
    "Q12308941": "male given name",
    "Q11879590": "female given name",
    "Q13442814": "scholarly article",
    "Q3305213": "painting"
}


def load_entity(line):
    line_stripped = line.rstrip(",\n")
    try:
        entity = json.loads(line_stripped)
    except:
        return

    return entity


def map_sitelinks(record, lang):
    entity = load_entity(record)

    if entity is None:
        return

    if entity["id"].startswith("P"):
        return

    wiki = "{}wiki".format(lang)

    if wiki not in entity["sitelinks"]:
        return

    qid = entity["id"]
    wiki_title = entity["sitelinks"][wiki]["title"]

    return "{}; {}".format(qid, wiki_title)


def get_sitelinks(sc, args):
    wikidata_rdd = sc.textFile(args.dump)

    wikidata_processed = wikidata_rdd.map(lambda x: map_sitelinks(x, args.lang)).filter(lambda x: x is not None)

    output_path = os.path.join(args.output, "sitelinks.{}".format(args.lang))
    wikidata_processed.repartition(1).saveAsTextFile(output_path)


def map_labels(record, lang):
    entity = load_entity(record)

    if entity is None:
        return

    if entity["id"].startswith("P"):
        return

    if lang not in entity["labels"]:
        return

    labels = [entity["labels"][lang]["value"]]

    for alias in entity["aliases"].get(lang, []):
        labels.append(alias["value"])

    qid = entity["id"]

    return "{}; {}".format(qid, ";;".join(labels))


def get_labels(sc, args):
    wikidata_rdd = sc.textFile(args.dump)

    wikidata_processed = wikidata_rdd.map(lambda x: map_labels(x, args.lang)).filter(lambda x: x is not None)

    output_path = os.path.join(args.output, "labels.{}".format(args.lang))
    wikidata_processed.repartition(1).saveAsTextFile(output_path)


def load_poject_pages(project_pages_file):
    '''
    Wikidata entities which are instances of Q14204246 (Wikimedia project page)
    This list was obtained with a SPARQL query
    '''
    pages = json.load(open(project_pages_file))
    qids = [x["item"].rsplit("/", 1)[-1] for x in pages]

    return qids


def is_good_entity_type(record, service_pages):
    entity = load_entity(record)

    if entity is None:
        return False

    if entity["id"].startswith("P"):
        return False

    object_types = entity["claims"].get(INSTANCE_OF, [])

    for item in object_types:
        item_qid = item.get("mainsnak", {}).get("datavalue", {}).get("value", {}).get("id", "")
        if item_qid in BAD_ENTITY_TYPES.keys() or item_qid in service_pages:
            return False

    return True


def map_neighbors(record):
    entity = load_entity(record)

    if entity is None:
        return

    qid = entity["id"]

    for pid, claims in entity["claims"].items():
        for snack in claims:
            if snack.get("mainsnak", {}).get("datatype", "") == "wikibase-item":
                neighbor_qid = snack["mainsnak"].get("datavalue", {}).get("value", {}).get("id", "")
                if neighbor_qid != "":
                    yield qid, ("out", pid, neighbor_qid)
                    yield neighbor_qid, ("in", pid, qid)


def collect_neighbors(record_group):
    result = {"in": {}, "out": {}}
    tmp = {"in": defaultdict(list), "out": defaultdict(list)}

    for record in record_group[1]:
        direction, prop, neighbor_qid = record
        if direction == "in":
            tmp["in"][prop].append(neighbor_qid)
        else:
            tmp["out"][prop].append(neighbor_qid)

    for d in ["in", "out"]:
        for prop, qids in tmp[d].items():
            if len(qids) <= 2:
                result[d][prop] = qids

    return json.dumps({record_group[0]: result})


def build_graph(sc, args):
    wikidata_rdd = sc.textFile(args.dump)

    if not args.project_pages:
        raise Exception("project_pages file is missing")

    wikidata_service_pages = load_poject_pages(args.project_pages) + list(WIKIDATA_SERVICE_PAGES.keys())

    wikidata_processed = wikidata_rdd\
        .filter(lambda x: is_good_entity_type(x, wikidata_service_pages))\
        .flatMap(map_neighbors)\
        .filter(lambda x: x is not None)\
        .groupByKey()\
        .map(collect_neighbors)

    output_path = os.path.join(args.output, "wikidata_graph")
    wikidata_processed.saveAsTextFile(output_path)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--lang", required=True, help="language")
    parser.add_argument("--dump", required=True, help="path to Wikidata dump file (HDFS)")
    parser.add_argument("--project-pages", help="path to Wikidata service pages file (local)")
    parser.add_argument("--output", required=True, help="path to output directory (HDFS)")
    parser.add_argument("--graph", action="store_true", help="build Wikidata graph as adjacency lists")
    parser.add_argument("--sitelinks", action="store_true", help="get sitelinks for language LANG")
    parser.add_argument("--labels", action="store_true", help="get labels and aliases for language LANG")

    args = parser.parse_args()

    log = set_logger("logs/process_wikidata_{}.{}".format(args.lang, int(time.time())))

    sc = SparkContext(appName="task")

    if args.sitelinks:
        log.info("Get sitelinks: start")
        get_sitelinks(sc, args)
        log.info("Get sitelinks: finish")

    if args.labels:
        log.info("Get labels: start")
        get_labels(sc, args)
        log.info("Get labels: finish")

    if args.graph:
        log.info("Build graph: start")
        build_graph(sc, args)
        log.info("Build graph: finish")


if __name__ == "__main__":
    main()