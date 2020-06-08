import argparse
from pyspark import SparkContext


def map_by_qid(record, lang):
    key, value = record
    wikidata_value, wikipedia_value = value
    qid, label = wikidata_value.split("#", 1)
    title, section = wikipedia_value.split("#", 1)

    res = "{};;{};;{};;{};;{}".format(qid, label, title, section, lang)

    return qid, res


def map_filter_unique(record):
    if len(record[1]) == 1:
        yield record[1][0]


def do_process(args):
    sc = SparkContext(appName="task")

    wikipedia_rdd = sc.sequenceFile(args.wikipedia)
    wikidata_rdd = sc.sequenceFile(args.wikidata)

    result = wikidata_rdd\
        .join(wikipedia_rdd)\
        .map(lambda x: map_by_qid(x, args.lang))\
        .groupByKey()\
        .mapValues(list)\
        .flatMap(map_filter_unique)

    result.repartition(1).saveAsTextFile(args.output)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--lang", required=True, help="language")
    parser.add_argument("--wikipedia", help="path to Wikipedia KV table (HDFS)")
    parser.add_argument("--wikidata", help="path to Wikidata KV table (HDFS)")
    parser.add_argument("--output", help="output path (HDFS)")

    args = parser.parse_args()

    do_process(args)


if __name__ == "__main__":
    main()
