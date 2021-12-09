Build the code:

./mvnw package


Download our full index from https://mega.nz/file/uihhhaTJ#uEgrhMts_YTGzlH1UkMXrdKmVIPHTRHsmhLZi58K6-A
This file is ~18 gigabytes.

Download a much smaller index from https://drive.google.com/file/d/19WILwAG6Pq_h06xZ3eutm2KAnviNA2I-/view?usp=sharing

ALL running codes are in engine file

To run a retrieval model:

java -jar target/ir-engine-0.jar <model> <index> <cbor-outlines> <qrel> {model-parameters} [filter | nofilter] <mergeType> <runfile>

<index> is the path to a database containing an index.  indexFull.db or indexOnlyScored.db provided by the above links.

<cbor-outlines> is the path to a cbor-outlines file to use for generating queries.  The query file we use is located at
documentretrievalengine/benchmarkY3test.cbor-outlines.cbor

<qrel> is the qrel file containing page relevance info used to evaluate our model.
The one we use is located at documentretrievalengine/trec_pages.qrel.

{model-parameters} is a list of parameters specific to each retrieval model.

if [filter] is specified, each facet will return the top 1000 scored documents, filter out all the documents that don't
evaluation data for that query, then return the top 20 of the resulting list.

<mergeType> is one of [ RoundRobin | Recurrance | RankRecurrance ]
This specifies how query facet results should be merged together.

<runfile> is the name of the output file containing query results in run file format.

Examples:

TF-IDF:
java -jar target/ir-engine-0.jar <model> <index> <cbor-outlines> <qrel> <ddd.qqq> [filter | nofilter] <mergeType> <runfile>

java -jar target/ir-engine-0.jar tfidf-cbor-query ./indexOnlyScored.db ../benchmarkY3test.cbor-outlines.cbor ../trec_pages.qrel btn.bnn nofilter RoundRobin tfidf-btn.bnn-smallIndex-nofilter-roundrobin.run

BM25:

java -jar target/ir-engine-0.jar <model> <index> <cbor-outlines> <qrel> <k1> <k3> <beta> [filter | nofilter] <mergeType> <runfile>

java -jar target/ir-engine-0.jar bm25-cbor-query ./indexOnlyScored.db ../benchmarkY3test.cbor-outlines.cbor ../trec_pages.qrel 1.0 0.8 0.5 nofilter RoundRobin tfidf-1.0_0.8_0.5-smallIndex-nofilter-roundrobin.run

calculate-vercoter:

java -jar target/ir-engine-0.jar <model> <index> <wordVectorfile>  <offset> <maxDocuments>

java -jar target/ir-engine-0.jar calculate-vectors indexOnlyScored.db preprocessed.6B.100d.txt 0 30000000

Cluster:

java -jar target/ir-engine-0.jar cluster indexOnlyScored.db 0 0 30000000   

WordSim:

java -jar target/ir-engine-0.jar <model> <index> <cbor-outlines> <qrel> <mergeType> <wordVectorfile> [filter | nofilter] <maxDocuments> <runfile>

java -jar target/ir-engine-0.jar wordSim-cbor-query indexOnlyScored.db benchmarkY3test.cbor-outlines.cbor trec_pages.qrel RoundRobin preprocessed.6B.100d.txt nofilter 30000000 wordSim-nonfilter.run 

BIM:

java -jar target/ir-engine-0.jar <model> <index> <cbor-outlines> <qrel> [filter | nofilter] <mergeType> <runfile>

java -jar target/ir-engine-0.jar bim indexOnlyScored.db benchmarkY3test.cbor-outlines.cbor trec_pages.qrel filter RoundRobin bim-RoundRobin-filter.run

java -jar target/ir-engine-0.jar bim indexOnlyScored.db benchmarkY3test.cbor-outlines.cbor trec_pages.qrel filter RankRecurrance bim-RankRecurrance-filter.run

java -jar target/ir-engine-0.jar bim indexOnlyScored.db benchmarkY3test.cbor-outlines.cbor trec_pages.qrel filter Recurrance bim-Recurrance-filter.run

java -jar target/ir-engine-0.jar bim indexOnlyScored.db benchmarkY3test.cbor-outlines.cbor trec_pages.qrel nofilter RoundRobin bim-RoundRobin-nofilter.run

java -jar target/ir-engine-0.jar bim indexOnlyScored.db benchmarkY3test.cbor-outlines.cbor trec_pages.qrel nofilter RankRecurrance bim-RankRecurrance-nofilter.run

java -jar target/ir-engine-0.jar bim indexOnlyScored.db benchmarkY3test.cbor-outlines.cbor trec_pages.qrel nofilter Recurrance bim-Recurrance-nofilter.run

Jelinek-Mercer:

java -jar target/ir-engine-0.jar <model> <index> <cbor-outlines> <qrel> <beta> [filter | nofilter] <mergeType> <runfile>


Evaluation:

Get the mean and stderr of each runfile using the following command:

documentretrievalengine/score.sh <measure>

where <measure> is the same as would be provided to the trec_eval utility.
For our report we used map:

./score.sh map