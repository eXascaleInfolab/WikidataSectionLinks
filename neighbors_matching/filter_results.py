import argparse
from collections import defaultdict
import editdistance
import time

from logger import set_logger


def do_filter(args, log):
    result = defaultdict(set)

    log.info("Loading results: start")
    with open(args.input) as fin:
        for line in fin:
            line = line.strip()
            qid, label, title, section, lang = line.split(";;")

            if len(section) == 0:
                log.warning("Empty section: {}".format(line))
                continue

            result[(qid, label, lang)].add((title, section))
    log.info("Loading results: finish")

    log.info("Filtering and saving results: start")
    with open(args.output, "w") as fout:
        for wd, wp_set in result.items():
            qid, label, lang = wd

            if len(wp_set) == 1:
                title, section = wp_set.pop()
                fout.write("{};;{};;{};;{};;{}\n".format(qid, label, title, section, lang))
            else:
                tmp = []
                for wp in wp_set:
                    title, section = wp
                    dist_section = editdistance.eval(label, section)
                    dist_title = editdistance.eval(label, title)
                    tmp.append((dist_section, dist_title, title, section))

                res_tmp = sorted(tmp, key=lambda x: (x[0], x[1]))

                fout.write("{};;{};;{};;{};;{}\n".format(qid, label, res_tmp[0][2], res_tmp[0][3], lang))
    log.info("Filtering and saving results: finish")


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)

    args = parser.parse_args()

    log = set_logger("logs/filter_res.{}".format(int(time.time())))

    do_filter(args, log)
    log.info("Filtering: finish")


if __name__ == "__main__":
    main()