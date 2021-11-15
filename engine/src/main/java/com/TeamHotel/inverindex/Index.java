package com.TeamHotel.inverindex;

import com.TeamHotel.main.WordSimilarity;
import com.TeamHotel.preprocessor.Preprocess;

import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class Index implements Serializable{
    private final Connection connection;
    private static final Map<QUERY, String> queryStrings;
    private boolean in_transaction = false;
    private AtomicInteger nextId = new AtomicInteger();
    private AtomicInteger nextPostingsId = new AtomicInteger();
    static {
        queryStrings = new HashMap<>();
        queryStrings.put(QUERY.INSERT_POSTINGS,                 "INSERT INTO POSTINGS VALUES (?, ?, 0, '')");
        queryStrings.put(QUERY.INSERT_DOCUMENT,                 "INSERT INTO DOCUMENTS VALUES (?, ?, ?, ?, ?, NULL, NULL, 0, 0, 0)");
        queryStrings.put(QUERY.INSERT_FAKE_LEADER,              "INSERT INTO DOCUMENTS VALUES (?, NULL, NULL, NULL, NULL, ?, ?, 1, 1, 0)");
        queryStrings.put(QUERY.UPDATE_POSTINGS,                 "UPDATE POSTINGS SET (DOC_FREQUENCY, POSTINGS_LIST) = (?, ?) WHERE TERM = ?");
        queryStrings.put(QUERY.UPDATE_DOCUMENT_VECTOR,          "UPDATE DOCUMENTS SET VECTOR=? WHERE DOCID=?");
        queryStrings.put(QUERY.UPDATE_DOCUMENT_CLASS,           "UPDATE DOCUMENTS SET CLASS=? WHERE DOCID=?");
        queryStrings.put(QUERY.UNSET_LEADERS,                   "UPDATE DOCUMENTS SET LEADER=0 WHERE LEADER=1");
        queryStrings.put(QUERY.MARK_SCORED,                     "UPDATE DOCUMENTS SET SCORED=1 WHERE DOCID=?");
        queryStrings.put(QUERY.DELETE_EXTRA_FAKES,              "DELETE FROM DOCUMENTS WHERE (FAKE=1 AND LEADER=0)");
        queryStrings.put(QUERY.DELETE_POSTINGS,                 "DELETE FROM POSTINGS");
        queryStrings.put(QUERY.SELECT_VECTOR_BY_INDEX,          "SELECT DOCID, VECTOR FROM DOCUMENTS WHERE (ID=?) AND (FAKE=0)");
        queryStrings.put(QUERY.SELECT_POSTINGS,                 "SELECT POSTINGS_LIST FROM POSTINGS WHERE TERM = ?");
        queryStrings.put(QUERY.SELECT_DOCUMENT_VECTOR_BY_CLASS, "SELECT DOCID, VECTOR FROM DOCUMENTS WHERE (CLASS=?) AND (FAKE=0)");
        queryStrings.put(QUERY.SELECT_LEADER_VECTORS,           "SELECT CLASS, VECTOR FROM DOCUMENTS WHERE (LEADER=1)");
        queryStrings.put(QUERY.SELECT_DOCUMENT_FULLTEXT,        "SELECT (FULL_TEXT) FROM DOCUMENTS WHERE (DOCID=?)");
        queryStrings.put(QUERY.SELECT_CLUSTERS,                 "SELECT DISTINCT (CLASS) FROM DOCUMENTS WHERE (CLASS !=0)");
        queryStrings.put(QUERY.COUNT_DOCUMENTS,                 "SELECT COUNT(*) FROM DOCUMENTS");
        queryStrings.put(QUERY.SELECT_ALL_DOCID_VECTOR_CLUSTER, "SELECT DOCID, VECTOR, CLASS FROM DOCUMENTS WHERE (FAKE=0) AND (ID >= ?) AND (ID < ?)");
        queryStrings.put(QUERY.SELECT_TOKENIZED_DOCUMENTS,      "SELECT DOCID, PREPROCESSED_TEXT FROM DOCUMENTS WHERE (FAKE=0) AND (ID >= ?) AND (ID < ?)");
        queryStrings.put(QUERY.SELECT_ALL_DOC_TF,               "SELECT DOCID, TOKEN_SET FROM DOCUMENTS WHERE (FAKE=0) AND (ID >= ?) AND (ID < ?)");
    }
    enum QUERY { INSERT_POSTINGS, SELECT_POSTINGS, INSERT_DOCUMENT, UPDATE_DOCUMENT_VECTOR, UPDATE_DOCUMENT_CLASS,
    INSERT_FAKE_LEADER, SELECT_DOCUMENT_VECTOR_BY_CLASS, SELECT_LEADER_VECTORS, SELECT_DOCUMENT_FULLTEXT, DELETE_EXTRA_FAKES,
    UNSET_LEADERS, SELECT_VECTOR_BY_INDEX, COUNT_DOCUMENTS, SELECT_CLUSTERS, SELECT_ALL_DOCID_VECTOR_CLUSTER, SELECT_TOKENIZED_DOCUMENTS,
    SELECT_ALL_DOC_TF, UPDATE_POSTINGS, DELETE_POSTINGS, MARK_SCORED }


    /**
     * Create an Index with a new database.
     * @param dbname - the name of the database.  Not the filename
     * @return - An Index with a connection to the created database - Or None if creating the Index fails.
     */
    @NotNull
    public static Optional<Index> createNew(@NotNull final String dbname) {
        try {
            Index idx = new Index(dbname, true);
            return Optional.of(idx);
        } catch (Exception ex) {
            ex.printStackTrace();
            return Optional.empty();
        }
    }

    /**
     * Create an Index from an existing database.
     * @param dbname - the name of the database.  Not the filename
     * @return - An Index with a connection to the specified database - Or None if creating the Index fails.
     */
    @NotNull
    public static Optional<Index> load(@NotNull final String dbname) {
        try {
            Index idx = new Index(dbname, false);
            return Optional.of(idx);
        } catch (Exception ex) {
            ex.printStackTrace();
            return Optional.empty();
        }
    }

    /**
     * private constructor for Index.
     * @param dbname - file to store the index.
     * @param resetIfExists - If true, we delete the relevant tables and rebuild an empty database.
     * @throws SQLException - If we can't connect to and initialize the database, we can't create an index.
     */
    private Index(@NotNull final String dbname, final boolean resetIfExists) throws SQLException {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
            throw new SQLException("Failed to initialize SQL library.  Class not found");
        }
        connection = DriverManager.getConnection(String.format("jdbc:sqlite:%s", dbname));
        if (resetIfExists) {
            resetDatabase();
        }
        // Verify all SQL statement strings are valid and match the schema.
        AtomicBoolean validSQL = new AtomicBoolean(true);
        queryStrings.forEach((t, s) -> {
            try {
                connection.prepareStatement(s).close();
            } catch (SQLException ex) {
                System.out.printf("Bad SQL: {%s}\n", s);
                //ex.printStackTrace();
                validSQL.set(false);
            }
        });
        if (!validSQL.get()) {
            throw new SQLException("SQL statements had errors or didn't match the database schema");
        }
    }

    private void resetDatabase() throws SQLException {
        try {
            PreparedStatement s = connection.prepareStatement("DROP TABLE DOCUMENTS");
            s.executeUpdate();
            s.close();
        } catch (SQLException ex) {
            //ex.printStackTrace();
            System.err.println("DOCUMENTS table does not exist");
        }
        try {
            PreparedStatement s = connection.prepareStatement("DROP TABLE POSTINGS");
            s.executeUpdate();
            s.close();
        } catch (SQLException ex) {
            //ex.printStackTrace();
            System.err.println("POSTINGS table does not exist");
        }

        PreparedStatement s = connection.prepareStatement(
        "CREATE TABLE POSTINGS " +
        "(ID             INT NOT NULL," +
        " TERM           TEXT PRIMARY KEY NOT NULL, " +
        " DOC_FREQUENCY  INT NOT NULL, " +
        " POSTINGS_LIST  TEXT NOT NULL" + 
        ")");
        s.executeUpdate();
        s.close();
        System.out.println("Created table POSTINGS");
        //ID, DOCID, FULLTEXT, PREPROCESSED_TOKENS, TOKEN_SET, CLASS=0, VECTOR=Array_all_0, LEADER=FALSE, FAKE=FALSE
        s = connection.prepareStatement(
        "CREATE TABLE DOCUMENTS (" +
            "ID INT   NOT NULL," +
            "DOCID TEXT PRIMARY KEY," +
            "FULL_TEXT TEXT," +
            "PREPROCESSED_TEXT TEXT," +
            "TOKEN_SET TEXT," +
            "CLASS INT," +
            "VECTOR TEXT," +
            "LEADER INT NOT NULL," +
            "FAKE INT NOT NULL)");
        s.executeUpdate();
        s.close();
        System.err.println("Created table DOCUMENTS");;
    }

    public void beginTransaction() {
        if (!in_transaction) {
            try {
                connection.prepareStatement("BEGIN TRANSACTION").execute();
            } catch (SQLException ex) {
                ex.printStackTrace();
                System.exit(-1);
            }
            in_transaction = true;
        } else {
            System.err.println("Transaction already in progress.  Remember to call commitTransaction()");
            System.exit(-1);
        }
    }  
    public void commitTransaction() {
        if (in_transaction) {
            try {
                connection.prepareStatement("COMMIT TRANSACTION").execute();
            } catch (SQLException ex) {
                ex.printStackTrace();
                System.exit(-1);
            }
            in_transaction = false;
        } else {
            System.err.println("No transaction in progress.  Remember to call beginTransaction()");
            System.exit(-1);
        }
    }

    /**
     * add a single document to the index
     * @param docID - the TREC id of the document
     * @param fulltext - a string containing the entire document
     * @param tokenizedText - the document represented by a list of preprocessed tokens
     * @param tokenSet - a map containing all unique tokens, and their frequencies
     */
    public void addNewDocument(@NotNull final String docID, @NotNull final String fulltext, @NotNull final List<String> tokenizedText,
    @NotNull final TreeMap<String, Integer> tokenSet) {
        try {
            @NotNull final PreparedStatement s = connection.prepareStatement(queryStrings.get(QUERY.INSERT_DOCUMENT));
            s.setInt(1, nextId.getAndIncrement());
            s.setString(2, docID);
            s.setString(3, fulltext);
            @NotNull final StringBuilder tokenString = new StringBuilder(tokenizedText.get(0));
            tokenizedText.listIterator(1).forEachRemaining(t -> tokenString.append(",").append(t));
            s.setString(4, tokenString.toString());
            final Map.Entry<String, Integer> first = tokenSet.firstEntry();
            if (first != null) {
                @NotNull final StringBuilder uniqueTokens = new StringBuilder(String.format("%s:%d", first.getKey(), first.getValue()));
                if (tokenSet.size() > 1) {
                    tokenSet.tailMap(tokenSet.higherKey(first.getKey())).forEach((t, tf) -> uniqueTokens.append(String.format(",%s:%d", t, tf)));
                }
                s.setString(5, uniqueTokens.toString());
            } else {
                s.close();
                return;
            }
            if (0 == s.executeUpdate()) {
                System.err.println("Failed to insert " + docID + " into database");
            }
            s.close();
        } catch(SQLException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Add all the documents in a TREC cbor paragraphs file into the index
     * @param cborParagraphs - file containing TREC cbor paragraphs
     * @param vocab - the set of words we care about.  The words should already be preprocessed and stemmed.
     * This is necessary because indexing will take far too long if we consider all words.
     */
    public int addNewDocuments(@NotNull final String cborParagraphs, @NotNull final Set<String> vocab, int offset, int max) {
        beginTransaction();
        nextId.set(offset);
        int numAdded = Preprocess.processParagraphs(cborParagraphs, (id, text, tokenized, tokens) -> {
            addNewDocument(id, text, tokenized, tokens);
        }, vocab, offset, max);
        commitTransaction();
        return numAdded;
    }

    /**
     * Log a query result for later analysis
     * @param queryID - the TREC id of the query
     * @param documentID - TREC id of returned document
     * @param score - score the document received
     * @param ranking - the document's ranking in overall query results
     */
    public void logResult(@NotNull final String queryID, @NotNull final String documentID, final double score, final int ranking) {
        final Optional<String> opt = getFulltextById(documentID);
        if (opt.isPresent()) {
            final String fulltext = opt.get();
            System.err.printf("Query %s - picked %s (%f-%d) (%s)\n", queryID, documentID, score, ranking, fulltext);
        } else {
            System.err.printf("Query %s - picked nonexistant document %s (%f-%d)\n", queryID, documentID, score, ranking);
        }
    }

    /**
     * retrieve the fulltext for a document.
     * @param documentID - the TREC id of the document
     * @return - the fulltext for the document stored in the index
     */
    @NotNull
    public Optional<String> getFulltextById(@NotNull final String documentID) {
        try {
            PreparedStatement s = connection.prepareStatement(queryStrings.get(QUERY.SELECT_DOCUMENT_FULLTEXT));
            s.setString(1, documentID);
            ResultSet result = s.executeQuery();
            if (result.next()) {
                final String str = result.getString("FULL_TEXT");
                s.close();
                return Optional.of(str);
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return Optional.empty();        
    }

    /**
     * Get the document vector for some document in the index.
     * You can use this to poll for random documents (using a random number).
     * Don't call this in a loop to retrieve all documents.  That would be very slow and error prone.
     * @param idx - a number representing the location to grab a document from.
     * This is not the TREC id, and these indices should not be used without good reason.
     * @return Pair<DocID, DocVector> if the specified document exists, else None
     */
    @NotNull
    public Optional<Pair<String, ArrayList<Double>>> getDocumentVectorByIndex(int idx) {
        try {
            PreparedStatement s = connection.prepareStatement(queryStrings.get(QUERY.SELECT_VECTOR_BY_INDEX));
            s.setInt(1, idx);
            ResultSet result = s.executeQuery();
            if (result.next()) {
                @NotNull final String id = result.getString("DOCID");
                final Scanner vectorScanner = new Scanner(result.getString("VECTOR"));
                final ArrayList<Double> vector = new ArrayList<>(300);
                vectorScanner.forEachRemaining((String d) -> vector.add(Double.parseDouble(d)));
                s.close();
                return Optional.of(Pair.of(id, vector));
            }
            s.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        } catch (NumberFormatException ex) {
            System.err.println("Malformed document vector in database");
            ex.printStackTrace();
        } catch (NullPointerException ex) {
            System.err.println("getDocumentVectorByIndex.  The supplied index did not point to a valid document");
        }
        return Optional.empty();
    }

    /**
     * count the number of documents in the index.
     * This does not count fake documents.
     * @return the number of real documents in the index
     */
    public int getNumDocuments() {
        try {
            PreparedStatement s = connection.prepareStatement(queryStrings.get(QUERY.COUNT_DOCUMENTS));
            ResultSet result = s.executeQuery();
            if (result.next()) {
                int numDocuments = result.getInt(1);
                result.close();
                return numDocuments;
            } else {
                result.close();
                throw new SQLException("Failed to count number of documents in index");
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return -1;
    }

    /**
     * add a fake cluster leader to the index.
     * After computing the centroid of a cluster we need to create a fake leader document to represent
     * the centroid in the next pass through the clustering algorithm.
     * @param clusterId - the cluster the leader belongs to
     * @param docVector - the vector representing the leader
     * @return true if the leader was successfully added
     */
    public boolean addFakeLeader(final int clusterId, @NotNull final ArrayList<Double> docVector) {
        try {
            final PreparedStatement s = connection.prepareStatement(queryStrings.get(QUERY.INSERT_FAKE_LEADER));
            final int id = nextId.getAndIncrement();
            final StringBuilder vectorString = new StringBuilder();
            docVector.forEach(d -> vectorString.append(String.format(" %f", d)));
            if (vectorString.length() > 0) {
                vectorString.deleteCharAt(0);
            }
            s.setInt(1, id);
            s.setInt(2, clusterId);
            s.setString(3, vectorString.toString());
            if (s.executeUpdate() == 1) {
                s.close();
                return true;
            }
            throw new SQLException("Failed to add fake leader into documents");
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return false;
    }

    /**
     * returns an iterator that iterates over all cluster leaders.
     * If a cluster has multiple leaders they will both be returned.
     * Remember to call clearLeaders() before assigning new cluster leaders.
     * To create a leader, call createFakeLeader(clusterId, vector).
     * @return - an iterator over all custer leaders
     */
    @NotNull
    public Iterator<Pair<Integer, ArrayList<Double>>> getClusterLeaders() {
        try {
            final PreparedStatement s = connection.prepareStatement(queryStrings.get(QUERY.SELECT_LEADER_VECTORS));
            ResultSet results = s.executeQuery();
            class LeaderIterator implements Iterable<Pair<Integer, ArrayList<Double>>> {
                boolean more = true;
                boolean getNext = true;
                Pair<Integer, ArrayList<Double>> curr = null;
                @Override
                public Iterator<Pair<Integer, ArrayList<Double>>> iterator() {
                    return new Iterator<Pair<Integer,ArrayList<Double>>>() {
                        @Override
                        public boolean hasNext() {
                            if (more) {
                                try {
                                    if (!getNext) {
                                        return curr != null;
                                    } else if (results.next()) {
                                        final int docClass = results.getInt("CLASS");
                                        final ArrayList<Double> vec = parseVector(results.getString("VECTOR"));
                                        curr = Pair.of(docClass, vec);
                                        getNext = false;
                                        return true;
                                    }
                                } catch (SQLException ex) {
                                    ex.printStackTrace();
                                }
                                more = false;
                                curr = null;
                                try {
                                    results.close();
                                } catch (SQLException ex) {
                                    ex.printStackTrace();
                                }
                            }
                            getNext = false;
                            return false;
                        }

                        @Override
                        @NotNull
                        public Pair<Integer, ArrayList<Double>> next() throws NoSuchElementException {
                            if (hasNext()) {
                                getNext = true;
                                return curr;
                            }
                            more = false;
                            throw new NoSuchElementException();
                        }
                    };
                }
            }
            return new LeaderIterator().iterator();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return Collections.emptyIterator();
    }

    /**
     * returns an iterator that iterates over all distinct cluster ids in the index.
     * Valid cluster ids are any integer > 0.
     * The words cluster and class are used interchangeably.
     * @return
     */
    @NotNull
    public Iterator<Integer> getClusters() {
        try {
            final PreparedStatement s = connection.prepareStatement(queryStrings.get(QUERY.SELECT_CLUSTERS));
            ResultSet results = s.executeQuery();
            class ClusterIterator implements Iterable<Integer> {
                boolean getNext = true;
                Integer curr = 0;
                boolean more = true;

                @Override
                public Iterator<Integer> iterator() {
                    return new Iterator<Integer>() {
                        @Override
                        public boolean hasNext() {
                            if (more) {
                                try {
                                    if (!getNext) {
                                        return curr != 0;
                                    } else if (results.next()) {
                                        curr = results.getInt("CLASS");
                                        getNext = false;
                                        return curr != 0;
                                    }
                                } catch (SQLException ex) {
                                    ex.printStackTrace();
                                }
                                more = false;
                                curr = 0;
                                getNext = false;
                                try {
                                    results.close();
                                } catch (SQLException ex) {
                                    ex.printStackTrace();
                                }
                            }
                            return false;
                        }

                        @Override
                        @NotNull
                        public Integer next() {
                            if (hasNext()) {
                                getNext = true;
                                return curr;
                            }
                            more = false;
                            throw new NoSuchElementException();
                        }
                    };
                }
            }
            return new ClusterIterator().iterator();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return Collections.emptyIterator();
    }

    /**
     * returns an iterator that iterates over all documents in a cluster
     * Valid cluster ids are any integer > 0.
     * The words cluster and class are used interchangeably.
     * It is possible for a cluster to be empty or very small depending on the clustering algorithm used.
     * It is also possible for clusters to be very large.  Make sure the code is generating clusters of a reasonable size.
     * @param clusterId - the id of the cluster to retrieve
     * @return - an iterator over all documents in the cluster.
     */
    @NotNull
    public Iterator<Pair<String, ArrayList<Double>>> getDocumentsInCluster(int clusterId) {
        try {
            final PreparedStatement s = connection.prepareStatement(queryStrings.get(QUERY.SELECT_DOCUMENT_VECTOR_BY_CLASS));
            s.setInt(1, clusterId);
            ResultSet results = s.executeQuery();
            class ClusterDocumentIterator implements Iterable<Pair<String, ArrayList<Double>>> {
                boolean getNext = true;
                boolean more = true;
                Pair<String, ArrayList<Double>> curr = null;
                @Override
                public Iterator<Pair<String, ArrayList<Double>>> iterator() {
                    return new Iterator<Pair<String,ArrayList<Double>>>() {
                        @Override
                        public boolean hasNext() {
                            if (more) {
                                try {
                                    if (!getNext) {
                                        return curr != null;
                                    } else if (results.next()) {
                                        @NotNull final String docId = results.getString("DOCID");
                                        @NotNull final ArrayList<Double> vec = parseVector(results.getString("VECTOR"));
                                        curr = Pair.of(docId, vec);
                                        getNext = false;
                                        return true;
                                    }
                                } catch (SQLException ex) {
                                    ex.printStackTrace();
                                }

                                more = false;
                                getNext = false;
                                curr = null;
                                try {
                                    results.close();
                                } catch (SQLException ex) {
                                    ex.printStackTrace();
                                }
                            }
                            return false;
                        }

                        @Override
                        public Pair<String, ArrayList<Double>> next() {
                            if (hasNext()) {
                                getNext = true;
                                return curr;
                            }
                            more = false;
                            throw new NoSuchElementException();
                        }
                        
                    };
                }
                
            }
            return new ClusterDocumentIterator().iterator();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return Collections.emptyIterator();
    }

    /**
     * returns an iterator that iterates over all documents in the specified range.
     * Using minIdx and maxDocuments you can can iterate over the corpus in parts.  Perhaps 10000 at a time.
     * @param minIdx - how many documents to skip before iterating, 0 starts at the first document.
     * @param maxDocuments - max number of documents to iterate over.
     * @return an iterator over the specified range of documents in the index.
     */
    @NotNull
    public Iterator<Triple<String, Integer, ArrayList<Double>>> getAllDocuments(final int minIdx, final int maxDocuments) {
        try {
            final PreparedStatement s = connection.prepareStatement(queryStrings.get(QUERY.SELECT_ALL_DOCID_VECTOR_CLUSTER));
            s.setInt(1, minIdx);
            s.setInt(2, minIdx + maxDocuments);
            ResultSet results = s.executeQuery();
            class AllDocumentIterator implements Iterable<Triple<String, Integer,  ArrayList<Double>>> {
                boolean more = true;
                boolean getNext = true;
                Triple<String, Integer, ArrayList<Double>> curr = null;
                @Override
                public Iterator<Triple<String, Integer, ArrayList<Double>>> iterator() {
                    return new Iterator<Triple<String, Integer, ArrayList<Double>>>() {
                        @Override
                        public boolean hasNext() {
                            if (more) {
                                try {
                                    if (!getNext) {
                                        return curr != null;
                                    } else if (results.next()) {
                                        @NotNull final String docId = results.getString("DOCID");
                                        final int classId = results.getInt("CLASS");
                                        final ArrayList<Double> vec = parseVector(results.getString("VECTOR"));
                                        curr = Triple.of(docId, classId, vec);
                                        getNext = false;
                                        return true;
                                    }
                                } catch (SQLException ex) {
                                    ex.printStackTrace();
                                }

                                more = false;
                                curr = null;
                                try {
                                    results.close();
                                } catch (SQLException ex) {
                                    ex.printStackTrace();
                                }
                            }
                            getNext = false;
                            return false;
                        }
                        @Override
                        public Triple<String, Integer, ArrayList<Double>> next() {
                            if (hasNext()) {
                                getNext = true;
                                return curr;
                            }
                            more = false;
                            throw new NoSuchElementException();
                        }   
                    };
                }
            }
            return new AllDocumentIterator().iterator();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return Collections.emptyIterator();
    }

    public Iterator<Pair<String, Map<String, Integer>>> getDocumentTermFrequencies(final int minIdx, final int maxDocuments) {
        try {
            final PreparedStatement s = connection.prepareStatement(queryStrings.get(QUERY.SELECT_ALL_DOC_TF));
            s.setInt(1, minIdx);
            s.setInt(2, minIdx + maxDocuments);
            ResultSet results = s.executeQuery();
            class AllTfIterator implements Iterable<Pair<String, Map<String, Integer>>> {
                boolean more = true;
                boolean getNext = true;
                Pair<String, Map<String, Integer>> curr = null;
                @Override
                public Iterator<Pair<String, Map<String, Integer>>> iterator() {
                    return new Iterator<Pair<String, Map<String, Integer>>>() {
                        @Override
                        public boolean hasNext() {
                            if (more) {
                                try {
                                    if (!getNext) {
                                        return curr != null;
                                    } else if (results.next()) {
                                        @NotNull final String docId = results.getString("DOCID");
                                        final Map<String, Integer> tfs = parseTermMap(results.getString("TOKEN_SET"));
                                        curr = Pair.of(docId, tfs);
                                        getNext = false;
                                        return true;
                                    }
                                } catch (SQLException ex) {
                                    ex.printStackTrace();
                                }

                                more = false;
                                curr = null;
                                try {
                                    results.close();
                                } catch (SQLException ex) {
                                    ex.printStackTrace();
                                }
                            }
                            getNext = false;
                            return false;
                        }
                        @Override
                        public Pair<String, Map<String, Integer>> next() {
                            if (hasNext()) {
                                getNext = true;
                                return curr;
                            }
                            more = false;
                            throw new NoSuchElementException();
                        }   
                    };
                }
            }
            return new AllTfIterator().iterator();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        System.out.println("Returning empty iterator");
        return Collections.emptyIterator();
    }

    public boolean setDocumentClass(@NotNull final String docId, final int clusterId) {
        try {
            final PreparedStatement s = connection.prepareStatement(queryStrings.get(QUERY.UPDATE_DOCUMENT_CLASS));
            s.setString(2, docId);
            s.setInt(1, clusterId);
            final int affectedRow = s.executeUpdate();
            s.close();
            if (affectedRow == 1) {
                return true;
            }
            System.err.printf("setDocumentClass() - Updated %d rows, expected 1\n", affectedRow);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return false;
    }

    public boolean setDocumentVector(@NotNull final String docID, @NotNull final ArrayList<Double> docVector) {
        try {
            final PreparedStatement s = connection.prepareStatement(queryStrings.get(QUERY.UPDATE_DOCUMENT_VECTOR));
            final String vecString = vectorString(docVector);
            s.setString(2, docID);
            s.setString(1, vecString);
            int affectedRows = s.executeUpdate();
            s.close();
            if (affectedRows == 1) {
                return true;
            }
            System.err.printf("Error setting document vector.  Expected 1 affected row, got %d\n", affectedRows);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return false;
    }

    public boolean markDocumentRelevant(@NotNull final String docID) {
        try {
            final PreparedStatement s = connection.prepareStatement(queryStrings.get(QUERY.MARK_SCORED));
            s.setString(1, docID);
            int affectedRows = s.executeUpdate();
            s.close();
            if (affectedRows == 1) {
                return true;
            }
            System.err.printf("Error setting document vector.  Expected 1 affected row, got %d\n", affectedRows);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return false;
    }

        /**
     * returns an iterator that iterates over all documents, providing the document id
     * @param offset - how many documents to skip before iterating.  0 will start at the first document.
     * @param maxDocuments - maximum number of documents to iterate over.  Set it really high to iterate over everything.
     * @return An iterator of Pair<DocID, Term-frequencies> over all documents in the specified range
     */
    @NotNull
    public Iterator<Pair<String, Map<String, Integer>>> getTokenizedDocuments(int offset, int maxDocuments) {
        try {
            final PreparedStatement s = connection.prepareStatement(queryStrings.get(QUERY.SELECT_TOKENIZED_DOCUMENTS));
            s.setInt(1, offset);
            s.setInt(2, offset + maxDocuments);
            final ResultSet results = s.executeQuery();
            class TokenizedIterator implements Iterable<Pair<String, Map<String, Integer>>> {
                boolean more = true;
                boolean getNext = true;
                Pair<String, Map<String, Integer>> curr = null;
                @Override
                public Iterator<Pair<String, Map<String, Integer>>> iterator() {
                    return new Iterator<Pair<String,Map<String,Integer>>>() {
                        @Override
                        public boolean hasNext() {
                            if (more) {
                                try {
                                    if (!getNext) {
                                        return curr != null;
                                    } else if (results.next()) {
                                        getNext = false;
                                        @NotNull final String docId = results.getString("DOCID");
                                        @NotNull final Map<String, Integer> terms = parseTermMapFromTokenizedText(results.getString("PREPROCESSED_TEXT"));
                                        curr = Pair.of(docId, terms);
                                        return true;
                                    }
                                } catch (SQLException ex) {
                                    ex.printStackTrace();
                                }
                                more = false;
                                curr = null;
                                try {
                                    results.close();
                                } catch (SQLException ex) {
                                    ex.printStackTrace();
                                }
                            }
                            getNext = false;
                            return false;
                        }

                        @Override
                        public Pair<String, Map<String, Integer>> next() {
                            if (hasNext()) {
                                getNext = true;
                                return curr;
                            }
                            more = false;
                            throw new NoSuchElementException();
                        }
                        
                    };
                }
                
            }
            return new TokenizedIterator().iterator();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return Collections.emptyIterator();
    }

    /**
     * Remove cluster leaders from the index.  Does not remove the actual documents, only their status as leaders.
     * It is possible that the number of clusters will reduce over time.  A little is okay, but if the number of clusters
     * halves, there is probably an issue with the algorithm.  The return value of this function indicates how many clusters
     * have at least one document.
     * @return the number of leaders removed from the database.
     */
    public int clearLeaders() {
        int affectedRows = 0;
        try {
            PreparedStatement s = connection.prepareStatement(queryStrings.get(QUERY.UNSET_LEADERS));
            affectedRows = s.executeUpdate();
            s.close();
            s = connection.prepareStatement(queryStrings.get(QUERY.DELETE_EXTRA_FAKES));
            s.execute();
            s.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return affectedRows;
    }

    public boolean updatePostingsList(@NotNull final String term, @NotNull final ArrayList<Pair<String, Integer>> postingsList, final int maxSize) {
        try {
            // combine old postings and new postings
            PostingsList oldPostings = getPostingsList(term, new TreeMap<>()).get();
            //postingsList.forEach((Pair<String, Integer> p) -> oldPostings.add(new IndexIdentifier(p.getRight(), p.getLeft()), null));

            // convert the list to Pair<String, Integer> for storage in database
            Iterator<Pair<IndexIdentifier, IndexDocument>> it = oldPostings.iterator();

            // create combined list of all documents
            final List<Pair<String, Integer>> postings = new ArrayList<>(maxSize * 2);
            it.forEachRemaining(p -> postings.add(Pair.of(p.getLeft().trecId, p.getLeft().termFrequency)));
            postings.addAll(postingsList);

            // sort descending by term frequency
            postings.sort((Pair<String, Integer> l, Pair<String, Integer> r) -> -Integer.compare(l.getRight(), r.getRight()));
            // filter duplicates
            List<Pair<String, Integer>> updatedPostings = postings.stream().distinct().collect(Collectors.toList());
            // truncate maxSize most occurring matches.  It would be better to consider document length as well,
            // I expect a large maxSize (10000)+ should be enough that we never need the tail of the list anyway.
            updatedPostings = updatedPostings.subList(0, Math.min(maxSize, updatedPostings.size()));

            @NotNull final PreparedStatement s = connection.prepareStatement(queryStrings.get(QUERY.UPDATE_POSTINGS));
            s.setInt(1, updatedPostings.size());
            final StringBuilder postingsStr = new StringBuilder(String.format("%s:%d", updatedPostings.get(0).getLeft(), updatedPostings.get(0).getRight()));
            updatedPostings.subList(0, updatedPostings.size()).forEach(p -> postingsStr.append(String.format(",%s:%d", p.getLeft(), p.getRight())));
            s.setString(2, postingsStr.toString());
            s.setString(3, term);
            return s.executeUpdate() == 1;
        } catch (SQLException ex) {
            ex.printStackTrace();
        } catch (NoSuchElementException ex) {
            ex.printStackTrace();
        }
        return false;
    }

    public Optional<PostingsList> getPostingsList(@NotNull final String term, @NotNull final Map<String, IndexDocument> documents) {
        try {
            final PreparedStatement s = connection.prepareStatement(queryStrings.get(QUERY.SELECT_POSTINGS));
            s.setString(1, term);
            ResultSet results = s.executeQuery();
            if (results.next()) {
                return Optional.of(PostingsList.fromString(term, results.getString("POSTINGS_LIST"), documents));
            }
        } catch (SQLException ex) {
            ex.toString();
        }
        System.err.printf("No postings list found for %s\n", term);
        return Optional.empty();
    }

    public Optional<PostingsList> getPostingsListForQuery(@NotNull final String term, @NotNull final Map<String, IndexDocument> documents) {
        try {
            final PreparedStatement s = connection.prepareStatement(queryStrings.get(QUERY.SELECT_POSTINGS));
            s.setString(1, term);
            ResultSet results = s.executeQuery();
            if (results.next()) {
                String st = results.getString("POSTINGS_LIST");
                PostingsList postings = PostingsList.fromString(term, st, documents);
                postings.truncate(1000);
                s.close();
                return Optional.of(postings);
            }
        } catch (SQLException ex) {
            ex.toString();
        }
        System.err.printf("No postings list found for %s\n", term);
        return Optional.empty();
    }

    public void deletePostings() {
        try {
            final PreparedStatement s = connection.prepareStatement(queryStrings.get(QUERY.DELETE_POSTINGS));
            s.executeUpdate();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public void addEmptyPostings(List<String> vocab) {
        vocab.forEach(word -> {
            try {
                final PreparedStatement s = connection.prepareStatement(queryStrings.get(QUERY.INSERT_POSTINGS));
                s.setInt(1, nextPostingsId.getAndIncrement());
                s.setString(2, word);
                s.executeUpdate();
                s.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        });
    }


    @NotNull
    private final ArrayList<Double> parseVector(@NotNull final String vectorString) throws SQLException {
        @NotNull final ArrayList<Double> vec = new ArrayList<>(WordSimilarity.numDimensions);
        try {
            @NotNull final Scanner vecScanner = new Scanner(vectorString);
            vecScanner.useDelimiter(",|\\s+");
            vecScanner.forEachRemaining(d -> vec.add(Double.parseDouble(d)));
            vecScanner.close();
            if (vec.size() == WordSimilarity.numDimensions) {
                return vec;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        throw new SQLException(String.format(
            "Document vector had %d dimensions, need %d\n", 
            vec.size(), WordSimilarity.numDimensions));
    }

    @NotNull
    private final String vectorString(@NotNull final ArrayList<Double> vector) throws IllegalArgumentException {
        if (vector.size() != WordSimilarity.numDimensions) {
            throw new IllegalArgumentException(
                String.format("Document vectors need %d dimensions, provided %d\n", 
                WordSimilarity.numDimensions, vector.size()));
        }
        final StringBuilder vecString = new StringBuilder();
        vector.forEach(d -> vecString.append(String.format(" %f", d)));
        if (vecString.length() > 0) {
            vecString.deleteCharAt(0);
        }
        return vecString.toString();
    }

    @NotNull
    private final Map<String, Integer> parseTermMap(@NotNull final String termString) throws SQLException {
        final TreeMap<String, Integer> termFrequencies = new TreeMap<>();
        Arrays.asList(termString.split(",")).stream().forEach(entryString -> {
            final String[] parts = entryString.split(":");
            final String term = parts[0];
            final int tf = Integer.parseInt(parts[1]);
            termFrequencies.put(term, tf);
        });
        return termFrequencies;
    }

    @NotNull
    private final Map<String, Integer>parseTermMapFromTokenizedText(@NotNull final String tokenizedText) throws SQLException {
        final TreeMap<String, Integer> termFrequencies = new TreeMap<>();
        Arrays.asList(tokenizedText.split(",")).stream().forEach(term -> {
            Integer occurrances = termFrequencies.putIfAbsent(term, 1);
            if (occurrances != null) {
                termFrequencies.put(term, occurrances + 1);
            }
        });
        return termFrequencies;
    }
}

