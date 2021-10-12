Build the code:

./mvnw package

Generate the .run files:

cd target


Run example:

Index TF-IDF:

java -jar prog-4.jar index ../collection-vocab.txt <cbor-paragraphfile> ../collection.index

Generated .run file for TF-IDF:

Format:

java -jar ./target/prog-4.jar cbor-query collection.index <cbor-outline> <AND|OR> <SMARTNotation>

Example:

java -jar ./target/prog-4.jar cbor-query collection.index train.pages.cbor-outlines.cbor AND anc.apc(SMART Notation)

java -jar ./target/prog-4.jar cbor-query collection.index train.pages.cbor-outlines.cbor OR anc.apc (SMAAT Notation)