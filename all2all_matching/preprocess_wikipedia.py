import argparse
from collections import defaultdict
from datetime import datetime
from xml.etree import ElementTree as ET

from pyspark import SparkContext

from generate_key import generate_key
from stopwords import get_stopwords


IRRELEVANT_SECTIONS = {"see also", "notes", "references", "further reading", "external links", "citations", "sources",
                       "primary sources", "secondary sources", "tertiary sources"}


def load_xml(file_name):
    with open(file_name) as fin:
        xml = ET.iterparse(fin)

        for event, el in xml:
            yield el


def process_page(page, sections_by_title):
    page_title = page.findtext("{http://www.mediawiki.org/xml/export-0.10/}title")
    redirect = page.find("{http://www.mediawiki.org/xml/export-0.10/}redirect")

    if redirect:
        return

    text = page.find("{http://www.mediawiki.org/xml/export-0.10/}revision").findtext(
        "{http://www.mediawiki.org/xml/export-0.10/}text")

    for line in text.split("\n"):
        line = line.strip()

        if line == "":
            continue

        if line.startswith("==") and line.endswith("=="):
            section_title = line.strip("= ")
            if section_title.lower() in IRRELEVANT_SECTIONS:
                continue

            sections_by_title[page_title].append(section_title)


def extract_sections(wiki_dump_file):
    wiki_xml = load_xml(wiki_dump_file)
    sections_by_title = defaultdict(list)

    i = 0
    for element in wiki_xml:
        if element.tag == "{http://www.mediawiki.org/xml/export-0.10/}page":
            i += 1
            process_page(element, sections_by_title)

            if i % 10000 == 0:
                print ("{}\t{} pages processed".format(datetime.now(), i))

            element.clear()

    return sections_by_title


def do_process(args):
    sc = SparkContext(appName="task")
    stop_words = get_stopwords(args.lang)

    sections_by_title = extract_sections(args.dump)

    dataset = []
    for title, sections in sections_by_title.items():
        for section in sections:
            key = generate_key(stop_words, title, section)
            value = "{}#{}".format(title, section)
            if key != "":
                dataset.append((key, value))

    rdd = sc.parallelize(dataset)
    rdd.saveAsSequenceFile(args.output)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--lang", required=True, help="language")
    parser.add_argument("--dump", required=True, help="path to Wikipedia dump file (local)")
    parser.add_argument("--output", required=True, help="path to Wikipedia KV table (HDFS)")

    args = parser.parse_args()

    do_process(args)


if __name__ == "__main__":
    main()
