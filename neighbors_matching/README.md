This method traverses the Wikidata graph and for those entities which are missing a Wikipedia sitelink it tries to find a match among neighbors' sitelinks.

Two preprocessing scripts (`preprocess_wikidata.py` and `preprocess_wikipedia.py`) generate files required for running the main algorithm (`main.py`). `filter_results.py` filters the output of `main.py` to remove duplicates and resolve non-unique mappings. 

### preprocess_wikipedia.py
Run locally. 

**Input:** Wikipedia dump file in xml format (download from https://dumps.wikimedia.org, `{lang}wiki-latest-pages-articles.xml.bz2` file).

**Output:** a JSON file containing a Python dict, where each key is a Wikipedia page title and value – a list of its sections (excluding irrelevant sections like "notes", "see also", "references" etc.).  
*Example*:  
```json
{
    "Lea Green railway station": [
        "History",
        "Facilities",
        "Services",
        "Gallery"
    ],
    "Kusuma Karunaratne": [
        "Personal life",
        "Academic career",
        "Publications",
        "Honors and awards"
    ],
}
```

**How to run:** `python3 preprocess_wikipedia.py --dump <path to Wikipedia dump file> --output <path to output file>`

### preprocess_wikidata.py
Run on Hadoop cluster.

**Input:** Wikidata dump file in JSON format uploaded to HDFS.

**Output:** 
* labels.{LANG} – labels and aliases in language LANG  
Format: one entity per line, line format: `qid; label;;alias_1;;alias_2;;alias_3` 
* sitelinks.{LANG} – sitelinks to {LANG}wiki  
Format: one entity per line, line format: `qid; Wikipedia page title` 
* wikidata_graph – a Wikidata graph represented with adjacency lists (each vertex contains references to its incoming and outgoing neighbors)  
*Example:* `{"Q59840720": {"out": {"P21": \["Q6581072"], "P106": \["Q1650915"], "P31": \["Q5"]}, "in": {"P50": \["Q59836242"]}}}` (one entity per line).  
**Note 1:** In this algorithm we don't consider neighbors which have more than 2 in/out edges of the same type, so those neighbors are filtered out on this step. For example, if an entity A has an outgoing edge of type "occupation" which links to an entity "singer", and 10 other enitities have this connection (`(X)-[occupation]->(singer)`), we discard "singer" from A's neighbors.  
**Note 2:** We also filter out entities which are instances of Wikimedia service pages and some other undesirable types. A list of Wikimedia service pages was obtained with a SPARQL query and is provided in `wikimedia_project_pages.gz` file.
**Note 3:** Wikidata graph is common for all language, it doesn't have to be rebuilt every time a new language is processed.

**How to run:** `spark-submit --master yarn --deploy-mode client preprocess_wikidata.py --lang LANG --dump <path to Wikidata dump file (HDFS)> --project-pages wikimedia_project_pages --output <path to folder where results will be saved> --graph --sitelinks --labels`  
Flags `--graph`, `--labels` and `--sitelinks` indicates which parts of the preprocessing should be run.

Important: all results should be downloaded from HDFS to the local filesystem, as the main algorithm is running locally (labels and sitelinks as individual files, graph as a directory).

### main.py
Run locally.

**Input:** 
* wikidata_labels.{LANG} (see **preprocess_wikidata.py** for details)
* wikidata_sitelinks.{LANG} (see **preprocess_wikidata.py** for details)
* wikidata_graph (see **preprocess_wikidata.py** for details)
* wikipedia_sections (see **preprocess_wikipedia.py** for details)

**Output:** a file containing mappings from Wikidata entities to Wikipedia sections (warning: mappings might be non-unique, run `filter_results.py` after obtaining results).  
Output format: `qid;;Wikidata label;;Wikipedia title;;Wikipedia section;;lang`

**How to run:** `python3 main.py --lang LANG --labels <path to Wikidata labels file> --sitelinks <path to Wikidata sitelinks file> --wd-graph <path to Wikidata graph dir> --wp-sections <path to Wikipedia sections file> --output <path to output file>`

### filter_results.py
Run locally.

**Input:** the output of `main.py`.

**Output:** a file in the same format as input with duplicate Wikidata entities removed (all `qid`s are unique).

**How to run:** `python3 filter_results.py --input INPUT --output OUTPUT`
