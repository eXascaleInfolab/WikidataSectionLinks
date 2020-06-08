This method performs the inner join of all Wikidata entities with all Wikipedia sections in a given language.  
All stages are run on a Hadoop cluster.

The algorithm proceeds in three steps:

### 1. Preprocess Wikipedia
**Input:** a Wikipedia dump file in xml format (download from https://dumps.wikimedia.org, {lang}wiki-latest-pages-articles.xml.bz2 file). The dump file should be stored on the local file system.

**Output:** A key-value pair RDD (stored on HDFS).  

*Example:* `(birth freefly olav zipser, Olav Zipser#The Birth of FreeFly)`
* A key is a string generated from a Wikipedia page title and a section title. Both titles are tokenized, stop-words and punctuation marks are removed, tokens are sorted alphabetically and concatenated back together.
* A value is a string containing the original page title and section title separated with # symbol.  

**How to run:** `spark-submit --master yarn --deploy-mode client preprocess_wikipedia.py --lang LANG --dump <path to Wikipedia dump file (local)> --output <path to output file (HDFS)>`

### 2. Preprocess Wikidata
**Input:** a Wikidata dump file in JSON format uploaded to HDFS.

**Output:** A key-value pair RDD (stored on HDFS).  

*Example:* 
```
(canton system, Q1728158#Canton system)
(kantonssystem, Q1728158#Canton system)
```
* A key is a string generated from a Wikidata entity label/aliases on a given language. For each alias a new entry is generated. A label/alias is tokenized, stop-words and punctuation marks are removed, tokens are sorted alphabetically and concatenated back together.
* A value is a string containing an entity QID and its label separated with # symbol.  

Important: only "orphan" entities are preserved in the output, i.e. entities which have a label in language LANG but don't have a sitelink to {LANG}wiki.

**How to run:** `spark-submit --master yarn --deploy-mode client preprocess_wikidata.py --lang LANG --dump <path to Wikidata dump file (HDFS)> --output <path to output file (HDFS)>`


### 3. Join
**Input:** a Wikipedia RDD and a Wikidata RDD generated at the previous steps.

**Output:** an HDFS text file containing mappings from Wikidata entities to Wikipedia sections. Mappings are unique, so no further filtering is required (filtering is performed on RDDs with the Spark operation `filter`).  
Output format: `qid;;Wikidata label;;Wikipedia title;;Wikipedia section;;lang`

**How to run:** `spark-submit --master yarn --deploy-mode client main.py --lang LANG --wikipedia <path to Wikipedia RDD (HDFS)> --wikidata <path to Wikidata RDD (HDFS)> --output <path to output file (HDFS)>`
