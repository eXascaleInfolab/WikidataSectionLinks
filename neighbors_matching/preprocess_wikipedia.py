import argparse
from collections import defaultdict
import json
import time
from xml.etree import ElementTree as ET

from logger import set_logger


IRRELEVANT_SECTIONS = {"see also", "notes", "references", "further reading", "external links", "citations", "sources", "primary sources", "secondary sources", "tertiary sources"}


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

    text = page.find("{http://www.mediawiki.org/xml/export-0.10/}revision").findtext("{http://www.mediawiki.org/xml/export-0.10/}text")

    for line in text.split("\n"):
        line = line.strip()

        if line == "":
            continue

        if line.startswith("==") and line.endswith("=="):
            section_title = line.strip("= ")
            if section_title.lower() in IRRELEVANT_SECTIONS:
                continue

            sections_by_title[page_title].append(section_title)


def extract_sections(wiki_dump_file, log):
    wiki_xml = load_xml(wiki_dump_file)
    sections_by_title = defaultdict(list)

    i = 0
    for element in wiki_xml:
        if element.tag == "{http://www.mediawiki.org/xml/export-0.10/}page":
            i += 1
            process_page(element, sections_by_title)

            if i % 10000 == 0:
                log.info("{} pages processed".format(i))

            element.clear()

    return sections_by_title


def process_wikipedia(args, log):
    log.info("Extract sections: start")
    sections_by_title = extract_sections(args.dump, log)
    log.info("Extract sections: finish")

    with open(args.output, "w") as fout:
        json.dump(sections_by_title, fout, ensure_ascii=False, indent=4)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--dump", required=True, help="path to Wikipedia dump file")
    parser.add_argument("--output", required=True, help="path to output file")

    args = parser.parse_args()

    log = set_logger("logs/process_wikipedia_{}.{}".format(args.lang, int(time.time())))

    process_wikipedia(args, log)


if __name__ == "__main__":
    main()