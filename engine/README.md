Build the code:

./mvnw package

MergeType:

RoundRobin
RankRecurrance
Recurrance

Example:

TF-IDF:

calculate-vercoter:
java -jar target/ir-engine-0.jar calculate-vectors indexOnlyScored.db preprocessed.6B.100d.txt 0 30000000      
Cluster:
java -jar target/ir-engine-0.jar cluster indexOnlyScored.db 0 0 30000000   
BIM:
java -jar target/ir-engine-0.jar bim indexOnlyScored.db benchmarkY3test.cbor-outlines.cbor trec_pages.qrel filter RoundRobin bim-RoundRobin-filter.run
java -jar target/ir-engine-0.jar bim indexOnlyScored.db benchmarkY3test.cbor-outlines.cbor trec_pages.qrel filter RankRecurrance bim-RankRecurrance-filter.run
java -jar target/ir-engine-0.jar bim indexOnlyScored.db benchmarkY3test.cbor-outlines.cbor trec_pages.qrel filter Recurrance bim-Recurrance-filter.run
java -jar target/ir-engine-0.jar bim indexOnlyScored.db benchmarkY3test.cbor-outlines.cbor trec_pages.qrel nofilter RoundRobin bim-RoundRobin-nofilter.run
java -jar target/ir-engine-0.jar bim indexOnlyScored.db benchmarkY3test.cbor-outlines.cbor trec_pages.qrel nofilter RankRecurrance bim-RankRecurrance-nofilter.run
java -jar target/ir-engine-0.jar bim indexOnlyScored.db benchmarkY3test.cbor-outlines.cbor trec_pages.qrel nofilter Recurrance bim-Recurrance-nofilter.run
B25:

Jelinek-Mercer:
