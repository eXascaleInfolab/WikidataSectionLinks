import argparse
import json

import urllib.parse


RDF_HEADERS = "@prefix schema: <http://schema.org/> .\n\
@prefix wd: <http://www.wikidata.org/entity/> . \n\
@prefix wikibase: <http://wikiba.se/ontology#> .\n\n"


def generate_wiki_url(lang, title, section):
    base_url = "https://{}.wikipedia.org/wiki".format(lang)

    path = "{}#{}".format(
        title.replace(" ", "_"),
        section.strip("[]'").replace(" ", "_")
    )

    url = "{}/{}".format(
        base_url,
        urllib.parse.quote(path)
    )

    return url


def convert_to_json(args):
    result = {}

    for f in args.input:
        with open(f) as fin:
            for line in fin:
                qid, _, title, section, lang = line.strip().split(";;")

                if len(section) == 0:
                    continue

                wiki = "{}wiki".format(lang)
                url = generate_wiki_url(lang, title, section)

                if qid not in result:
                    result[qid] = {
                        "id": qid,
                        "sitelinks": {}
                    }

                result[qid]["sitelinks"][wiki] = {
                    "site": wiki,
                    "title": "{}#{}".format(title, section),
                    "url": url,
                }

    with open(args.output, "w") as fout:
        fout.write("[\n")
        for qid, value in result.items():
            fout.write(json.dumps(value, ensure_ascii=False))
            fout.write(",\n")
        fout.write("]")


def convert_to_rdf(args):
    result = {}

    for f in args.input:
        with open(f) as fin:
            for line in fin:
                qid, _, title, section, lang = line.strip().split(";;")

                if len(section) == 0:
                    continue

                wiki = "{}wiki".format(lang)
                url = generate_wiki_url(lang, title, section)

                if qid not in result:
                    result[qid] = {
                        "id": qid,
                        "sitelinks": {}
                    }

                result[qid]["sitelinks"][wiki] = {
                    "site": wiki,
                    "title": "{}#{}".format(title, section),
                    "url": url
                }

    with open(args.output, "w") as fout:
        fout.write(RDF_HEADERS)

        for qid, data in result.items():
            for wiki, sitelink in data["sitelinks"].items():
                lang = wiki[:-4]
                fout.write("<{}> a schema:Article ;\n".format(sitelink["url"]))
                fout.write("	schema:about wd:{} ;\n".format(qid))
                fout.write("	schema:inLanguage \"{}\" ;\n".format(lang))
                fout.write("	schema:isPartOf <https://{}.wikipedia.org/> ;\n".format(lang))
                fout.write("	schema:name \"{}\"@{} .\n\n".format(sitelink["title"], lang))
                fout.write("<https://{}.wikipedia.org/> wikibase:wikiGroup \"wikipedia\" .\n\n".format(lang))


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument("--input", nargs="+", help="Files with Wikidata-Wikipedia mapping.\n"
                                                   "Files contain lines in the following format:\n"
                                                   "\"qid;;wikidata_label;;wikipedia_page_title;;wikipedia_section_title;;lang\"\n"
                                                   "Duplicates are overwritten.")
    parser.add_argument("--format", choices=["rdf", "json"], required=True, help="output format")
    parser.add_argument("--output", required=True, help="path to output file")

    args = parser.parse_args()

    if args.format == "rdf":
        convert_to_rdf(args)
    else:
        convert_to_json(args)


if __name__ == "__main__":
    main()
