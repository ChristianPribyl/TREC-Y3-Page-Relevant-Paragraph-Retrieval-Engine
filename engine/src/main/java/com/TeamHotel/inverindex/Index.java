package com.TeamHotel.inverindex;

import com.TeamHotel.preprocessor.Preprocess;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.FileInputStream;


public class Index implements Serializable{
    public String carId;
    public ConcurrentHashMap<String, Integer> termsSet;
    public IndexDocument indexDocument;

    private final Connection connection;
    private final PreparedStatement insertPostingsStatement;
    private final PreparedStatement selectPostingsStatement;
    private final PreparedStatement insertIndexDocument;
    private final PreparedStatement selectIndexDocument;
    private final PreparedStatement insertDocument;
    private final PreparedStatement updateDocumentVector;
    private final PreparedStatement updateDocumentClass;
    private final PreparedStatement insertFakeLeader;
    private final PreparedStatement selectDocumentVectorsByClass;
    private final PreparedStatement selectDocumentLeaderVectors;
    private final PreparedStatement selectDocumentFulltext;
    private final PreparedStatement deleteExtraFakes;
    private final PreparedStatement unsetLeaders;
    private boolean in_transaction = false;
    private AtomicInteger nextId = new AtomicInteger();



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

    private Index(@NotNull final String dbname, final boolean resetIfExists) throws SQLException, ClassNotFoundException {
        Class.forName("org.sqlite.JDBC");
        connection = DriverManager.getConnection(String.format("jdbc:sqlite:%s.db", dbname));
        if (resetIfExists) {
            resetDatabase();
        }
        //ID, DOCID, FULLTEXT, PREPROCESSED_TOKENS, TOKEN_SET, CLASS="", VECTOR=Array_all_0, LEADER=FALSE, FAKE=FALSE
        insertDocument = connection.prepareStatement(
            "INSERT INTO DOCUMENTS VALUES (?, ?, ?, ?, ?, NULL, NULL, 0, 0)");
        updateDocumentVector = connection.prepareStatement(
            "UPDATE DOCUMENTS SET VECTOR=? WHERE DOCID=?");
        updateDocumentClass = connection.prepareStatement(
            "UPDATE DOCUMENTS SET CLASS=? WHERE DOCID=?");
        insertFakeLeader = connection.prepareStatement(
            "INSERT INTO DOCUMENTS VALUES (?, NULL, NULL, NULL, NULL, ?, ?, 1, 1)");
        selectDocumentVectorsByClass = connection.prepareStatement(
            "SELECT (DOCID, VECTOR) FROM DOCUMENTS WHERE CLASS=?");
        selectDocumentLeaderVectors = connection.prepareStatement(
            "SELECT (DOCID, VECTOR) FROM DOCUMENTS WHERE LEADER=1");
        selectDocumentFulltext = connection.prepareStatement(
            "SELECT (FULL_TEXT) FROM DOCUMENTS WHERE DOCID=?");
        deleteExtraFakes = connection.prepareStatement(
            "DELETE FROM DOCUMENTS WHERE (FAKE=1 && LEADER=0)");
        unsetLeaders = connection.prepareStatement(
            "UPDATE DOCUMENTS SET LEADER=0 WHERE LEADER=1");

        // ID, TERM, DOCUMENT_FREQUENCY, POSTINGS
        insertPostingsStatement = connection.prepareStatement(
            "INSERT INTO POSTINGS VALUES (?, ?, ?, ?)");
        selectPostingsStatement = connection.prepareStatement(
            "SELECT POSTINGS_LIST FROM POSTINGS WHERE TERM = ?");
        insertIndexDocument = connection.prepareStatement(
            "INSERT INTO INDEX VALUES (?, ?, ?, ?)");
        selectIndexDocument = null;//connection.prepareStatement(
            //"SELECT INDEX_ENTRY FROM DOCUMENTS WHERE DOCID = ?");
    }

    private void resetDatabase() throws SQLException {
        try {
            PreparedStatement s = connection.prepareStatement("DROP TABLE INDEX");
            s.executeUpdate();
            s.close();
        } catch (SQLException ex) {
            //ex.printStackTrace();
            System.err.println("INDEX table does not exist");
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
        " DOC_FREQUENCY  INT     NOT NULL, " +
        " POSTINGS_LIST  BINARY  NOT NULL" + 
        ")");
        s.executeUpdate();
        s.close();
        System.out.println("Created table POSTINGS");
        //ID, DOCID, FULLTEXT, PREPROCESSED_TOKENS, TOKEN_SET, CLASS=0, VECTOR=Array_all_0, LEADER=FALSE, FAKE=FALSE
        s = connection.prepareStatement(
        "CREATE TABLE DOCUMENTS (" +
            "ID INT NOT NULL," +
            "DOCID TEXT PRIMARY KEY NOT NULL," +
            "FULL_TEXT TEXT," +
            "PREPROCESSED_TEXT TEXT," +
            "TOKEN_SET TEXT," +
            "CLASS INT," +
            "VECTOR TEXT," +
            "LEADER BIT NOT NULL," +
            "FAKE BIT NOT NULL)");
        s.executeUpdate();
        s.close();
        System.err.println("Created table INDEX");;
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

    public void addNewDocument(@NotNull final String docID, @NotNull final String fulltext, @NotNull final List<String> tokenizedText,
    @NotNull final TreeMap<String, Integer> tokenSet) {
        try {
            insertDocument.setInt(1, nextId.getAndIncrement());
            insertDocument.setString(2, docID);
            insertDocument.setString(3, fulltext);
            final StringBuilder tokenString = new StringBuilder(tokenizedText.get(0));
            tokenizedText.listIterator(1).forEachRemaining(t -> tokenString.append(",").append(t));
            insertDocument.setString(4, tokenString.toString());
            final StringBuilder uniqueTokens = new StringBuilder();
            tokenSet.forEach((t, tf) -> uniqueTokens.append(",").append(t).append(":").append(tf));
            uniqueTokens.subSequence(1, uniqueTokens.length());
            insertDocument.setString(5, uniqueTokens.toString());
            if (0 == insertDocument.executeUpdate()) {
                System.err.println("Failed to insert " + docID + " into database");
            }
        } catch(SQLException ex) {
            ex.printStackTrace();
        }
    }

    public void addNewDocuments(@NotNull final String cborParagraphs, @NotNull final Set<String> vocab) {
        Preprocess.processParagraphs(cborParagraphs, (id, text, tokenized, tokens) -> {
            addNewDocument(id, text, tokenized, tokens);
        }, vocab, 0, 100000000);
    }

    public void logResult(String queryID, String left, Double right, int i) {
    }

    public com.TeamHotel.inverindex.IndexDocument getDocumentByIndex(int randomNum) {
        return null;
    }

    public int getNumDocuments() {
        return 0;
    }

    public void addFakeLeader(int clusterId, int[] docVector) {
    }

    public Iterator<Triple<String, ArrayList<Double>, Integer>> getClusterLeaders() {
        return null;
    }

    public Iterator<Integer> getClusters() {
        return null;
    }

    public Iterator<Pair<String, ArrayList<Double>>> getDocumentsInCluster(int i, int j) {
        return null;
    }

    public Iterator<Triple<String, ArrayList<Double>, Integer>> getAllDocuments(int i, int j) {
        return null;
    }

    public void removeUnusedFakeDocuments() {
    }

    public void setDocumentClass(String docId, int clusterId) {
    }

    public Iterator<Integer> getClusterIds() {
        return null;
    }

    public Iterator<Pair<String, Map<String, Integer>>> getTokenizedDocuments(int offset, int i) {
        return null;
    }

    public void setDocumentVector(String docID, double[] docVector) {
    }

}

