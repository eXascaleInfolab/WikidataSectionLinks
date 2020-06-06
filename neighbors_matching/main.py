from argparse import ArgumentParser
import json
import os
import re
from string import punctuation
import time

from logger import set_logger

import nltk
nltk.download("stopwords")
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer, cistem, arlstem, snowball

from pymystem3 import Mystem
mystem = Mystem()

INSTANCE_OF = "P31"

STOPWORDS_KEY = {
    "ar": "arabic",
    "da": "danish",
    "de": "german",
    "en": "english",
    "es": "spanish",
    "fi": "finnish",
    "fr": "french",
    "hu": "hungarian",
    "id": "indonesian",
    "it": "italian",
    "nl": "dutch",
    "no": "norwegian",
    "pt": "portuguese",
    "ro": "romanian",
    "ru": "russian",
    "sv": "swedish"
}

KEEP_STOPWORDS = ["other"]

STEMMERS = {
    "ar": arlstem.ARLSTem,
    "en": PorterStemmer,
    "de": cistem.Cistem,
    "es": snowball.SpanishStemmer,
    "fr": snowball.FrenchStemmer,
    "it": snowball.ItalianStemmer,
    "nl": snowball.DutchStemmer,
    "pt": snowball.PortugueseStemmer,
    "sv": snowball.SwedishStemmer
}


HOMOGENEOUSITY_LEVEL = 2  # How many edges of the same type are allowed

PROTEIN = "Q8054"
BAD_ALIASES = {"uncharacterized protein", "hypothetical protein", "protein"}


def load_labels(labels_file):
    res = dict()

    with open(labels_file) as fin:
        for line in fin:
            line = line.strip()
            qid, labels = line.split("; ", 1)
            res[qid] = labels.split(";;")

    return res


def load_sitelinks(sitelinks_file):
    res = dict()

    with open(sitelinks_file) as fin:
        for line in fin:
            line = line.strip()
            qid, title = line.split("; ", 1)
            res[qid] = title

    return res


def load_graph(graph_dir):
    res = dict()
    neighbors_files = os.listdir(graph_dir)

    for f in neighbors_files:
        with open(os.path.join(graph_dir, f)) as fin:
            for line in fin:
                line = line.strip()
                res.update(json.loads(line))

    return res


def is_orphan(qid, labels, sitelinks):
    return (qid in labels) and (qid not in sitelinks)


def preprocess_string(s, lang, stop_words, stemmer):
    if lang == "ru":
        tokens = mystem.lemmatize(s.lower())
    else:
        tokens = s.lower().split(" ")

    tokens = [token.strip(punctuation) for token in tokens if token not in stop_words
              and token != " "
              and token.strip() not in punctuation]

    if stemmer is not None:
        tokens = [stemmer.stem(w) for w in tokens]

    return tokens


def generate_wikidata_key(label, lang, stop_words, stemmer):
    tokens = preprocess_string(label, lang, stop_words, stemmer)
    if len(tokens) == 0:
        return ""

    return " ".join(sorted(tokens))


def generate_wikipedia_key(title, section, lang, stop_words, stemmer):
    title = re.sub(" \([^)]+\)$", "", title)  # remove disambiguation phrase

    title_tokens = preprocess_string(title, lang, stop_words, stemmer)
    section_tokens = preprocess_string(section, lang, stop_words, stemmer)

    key_1 = " ".join(sorted(section_tokens))
    key_2 = " ".join(sorted(section_tokens + title_tokens))

    return key_1, key_2


def match_qid(qid, labels, sitelinks, wikidata, wikipedia, lang, stop_words, stemmer):
    results = []

    wikidata_keys = [generate_wikidata_key(label, lang, stop_words, stemmer) for label in labels[qid]]
    wikidata_keys = set(filter(lambda x: x != "", wikidata_keys))

    if len(wikidata_keys) == 0:
        return results

    for direction in ["in", "out"]:
        for prop, neighbor_qids in wikidata[qid][direction].items():
            if len(neighbor_qids) > HOMOGENEOUSITY_LEVEL:
                continue

            for neighbor_qid in neighbor_qids:
                if neighbor_qid not in sitelinks:
                    continue

                title = sitelinks[neighbor_qid]
                if title not in wikipedia:
                    continue

                for section in wikipedia[title]:
                    if len(section) == 1:
                        continue
                    key_section, key_full = generate_wikipedia_key(title, section, lang, stop_words, stemmer)

                    if key_section in wikidata_keys or key_full in wikidata_keys:
                        results.append("{};;{};;{};;{};;{}\n".format(
                            qid,
                            labels[qid][0],
                            title,
                            section,
                            lang
                        ))

    return results


def do_process(args, log):
    log.info("Load wikidata labels: start")
    labels = load_labels(args.labels)
    log.info("Load wikidata labels: finish")

    log.info("Load wikidata sitelinks: start")
    sitelinks = load_sitelinks(args.sitelinks)
    log.info("Load wikidata sitelinks: finish")

    log.info("Load wikidata neighbors: start")
    wikidata = load_graph(args.wd_graph)
    log.info("Load wikidata neighbors: finish")

    log.info("Load wikipedia sections: start")
    wikipedia = json.load(open(args.wp_sections))
    log.info("Load wikipedia sections: finish")

    stemmer = STEMMERS.get(args.lang, None)

    if args.lang in STOPWORDS_KEY:
        stop_words = set(stopwords.words(STOPWORDS_KEY[args.lang]))
    else:
        stop_words = set()

    # Stop words to keep (e.g. "other" in English coincides with a section name and should be kept)
    for w in KEEP_STOPWORDS:
        if w in stop_words:
            stop_words.remove(w)

    log.info("Wikidata graph stats: {} vertices".format(len(wikidata)))

    vertices = 0
    orphan_vertices = 0

    with open(args.output, "w") as fout:
        for qid, neighbors in wikidata.items():
            vertices += 1

            if is_orphan(qid, labels, sitelinks):
                orphan_vertices += 1

                object_type = set(neighbors["out"].get(INSTANCE_OF, []))

                # ad hoc filtering
                if PROTEIN in object_type:
                    labels[qid] = list(set(labels[qid]) - BAD_ALIASES)

                match_res = match_qid(qid, labels, sitelinks, wikidata, wikipedia, args.lang, stop_words, stemmer)

                for l in match_res:
                    fout.write(l)

            if vertices % 10000 == 0:
                log.info("Vertices processed: {}. Orphans: {}".format(vertices, orphan_vertices))


def main():
    parser = ArgumentParser()

    parser.add_argument("--lang", required=True, help="language")
    parser.add_argument("--labels", required=True, help="path to file with Wikidata labels and aliases for language LANG")
    parser.add_argument("--sitelinks", required=True, help="path to file with Wikidata sitelinks for language LANG")
    parser.add_argument("--wd-graph", required=True, help="path to Wikidata graph (represented with adjacency lists)")
    parser.add_argument("--wp-sections", required=True, help="path to file with Wikipedia sections")
    parser.add_argument("--output", required=True, help="path to output file")

    args = parser.parse_args()

    log = set_logger("logs/match_{}.{}".format(args.lang, int(time.time())))

    do_process(args, log)


if __name__ == "__main__":
    main()
