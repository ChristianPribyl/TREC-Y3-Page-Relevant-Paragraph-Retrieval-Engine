Linux (apt) setup:

1. Install java 11 or later
`sudo apt install openjdk-11-jdk`

# I am using 'openjdk 11.0.12 2021-07-20'

2. Set JAVA_HOME
`export JAVA_HOME="/usr"
# This is likely correct if you installed the jdk with apt.
# The executables $JAVA_HOME/bin/java and $JAVA_HOME/bin/javac must exist.
# Find the location of those executables with:
`which javac` and `which java`

# Add the export command to your .bashrc or .bash_profile for
# building java applications later in the future.

3. Build the application
`./mvnw package`
# execute from the project root directory.
# The executable jar is located at `target/Assignment1-<version>.jar`

4. Run the program

4.1. Create the index
`java -jar Assignment1-1.0.jar index <cbor-paragraphs.cbor> <index-directory>`

4.2. Run the cbor queries
`java -jar Assignment1-1.0.jar query <cbor-outline.cbor> <index-directory>`
This creates the test200-lucene-defaultranking.run

4.3. Run manual queries
`java -jar Assignment1-1.0.jar bench-query <index-directory>`
Enter queries using the interactive terminal app.
