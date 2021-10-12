./mvnw clean package
cd target
printf("create-index.sh <cbor-paragraphs.cbor>")
java -jar Assignment1-1.0.jar index %1 .