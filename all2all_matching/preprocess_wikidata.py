import argparse
import json
from pyspark import SparkContext

from generate_key import generate_key
from stopwords import get_stopwords


def is_orphan(lang, entity):
    return (lang in entity["labels"]) and ("{}wiki".format(lang) not in entity["sitelinks"])


def map_preprocess_wikidata(record, lang, stop_words):
    line_stripped = record.rstrip(",\n")
    try:
        entity = json.loads(line_stripped)
    except:
        return

    qid = entity["id"]
    if qid.startswith("P"):
        return

    if is_orphan(lang, entity):
        label_main = entity["labels"][lang]["value"]
        aliases = [alias["value"] for alias in entity["aliases"].get(lang, [])]

        labels = [label_main] + aliases

        for label in labels:
            key = generate_key(stop_words, label)
            value = "{}#{}".format(qid, label_main)

            if key != "":
                yield key, value


def do_process(args):
    sc = SparkContext(appName="task")
    sc.addPyFile("generate_key.py")

    stop_words = get_stopwords(args.lang)

    rdd = sc.textFile(args.dump)

    rdd_processed = rdd.flatMap(lambda x: map_preprocess_wikidata(x, args.lang, stop_words))

    rdd_processed.saveAsSequenceFile(args.output)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--lang", required=True)
    parser.add_argument("--dump", required=True, help="path to Wikidata dump file (HDFS)")
    parser.add_argument("--output", required=True, help="path to Wikidata KV table (HDFS)")

    args = parser.parse_args()

    do_process(args)


if __name__ == "__main__":
    main()
