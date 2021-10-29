package com.TeamHotel.inverindex;


import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.sql.*;

import com.TeamHotel.preprocessor.Preprocess;

import org.apache.commons.lang3.SerializationUtils;
import org.jetbrains.annotations.NotNull;

public class InvertedIndex implements Serializable {
    private final ConcurrentHashMap<String, PostingsList> index;
    private final ConcurrentHashMap<String, IndexDocument> documents;
    private Connection connection;
    private static AtomicInteger nextID = new AtomicInteger();
    private PreparedStatement insertPostingsStatement;
    private PreparedStatement selectPostingsStatement;
    private PreparedStatement insertIndexDocument;
    private PreparedStatement selectIndexDocument;

    public InvertedIndex() {
        index = null;
        documents = null;

    }
    public InvertedIndex(ConcurrentHashMap<String, PostingsList> idx,ConcurrentHashMap<String, IndexDocument> docs) {
        System.err.println("Initializing in-memory index");
        this.index = idx;
        this.documents = docs;
    }

   public ConcurrentHashMap<String, PostingsList> getIndex() {
        return index;
    }

    public int numTerms() {
        return index.size();
    }

    public int numDocuments() {
        return documents.size();
    }

    public PostingsList getPostingsList(@NotNull String term) {
        if (term.equals("")) {
            return null;
        }
        return index.getOrDefault(term, null);
    }

    public static InvertedIndex createInvertedIndex(final String vocabFile, final String cborParagraphs, int offset, int maxParagraphs) {
        InvertedIndex index = new InvertedIndex();
        index.connectToDatabase("../index.db");
        
        int numThreads = 6;
        //make a hashtable chain with linked list 
        // key: vocab, values will be list of docID which the document that has vocab word.
        ConcurrentHashMap<String, PostingsList> invertedIndex = new ConcurrentHashMap<>(200000);
        Set<String> vocab = new HashSet<>(500);
        try {
            Scanner vocabScanner = new Scanner(new FileInputStream(vocabFile));
            vocabScanner.forEachRemaining(w -> vocab.add(w));
        } catch (IOException ex) {
            ex.printStackTrace();
            return null;
        }
        
        ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> documents = Preprocess.preprocessLargeCborParagrphsWithVocab(cborParagraphs, vocab, offset, maxParagraphs);
        System.out.println("Allocating space for index documents");
        ConcurrentHashMap<String, IndexDocument> indexDocuments = new ConcurrentHashMap<>(40000000);
        System.out.println("Initializing index documents");
        documents.forEach((id, terms) -> indexDocuments.put(id, new IndexDocument(id)));
        //ArrayList<Index> index = Index.createIndex(cborParagraphs);
        System.out.println("Generating inverted index");
        List<List<String>> wordLists = new ArrayList<List<String>>(numThreads);
        for (int i = 0; i < numThreads; i++) {
            wordLists.add(new LinkedList<String>());
        }

        final AtomicInteger atomicI = new AtomicInteger(0);
        vocab.forEach(w -> wordLists.get(atomicI.incrementAndGet() % numThreads).add(w));
 
        Thread[] threads = new Thread[numThreads];
        AtomicInteger tids = new AtomicInteger(0);
        AtomicInteger indexedWords = new AtomicInteger(0);
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(new Runnable(){
                public void run() {
                    int tid = tids.incrementAndGet() - 1;
                    wordLists.get(tid).forEach(word -> {
                        PostingsList postings = new PostingsList(word);
                        documents.forEach((id, terms) -> {
                            int tf = terms.getOrDefault(word, 0);
                            if (tf != 0) {
                                postings.add(new IndexIdentifier(tf, id), indexDocuments.get(id));
                            }});
                        invertedIndex.put(word, postings);
                        int n = indexedWords.incrementAndGet();
                        if (n % 100 == 0) {
                            System.out.printf("Indexed %d terms\n", n);
                        }
                    });
                }});
            threads[i].start();
        }

        for (int i = 0; i < numThreads; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException ex) {
                ex.printStackTrace();
                return null;
            }
        }

        return new InvertedIndex(invertedIndex, indexDocuments);
    }

    public static InvertedIndex loadInvertedIndex(@NotNull final String invIndexFile) {
        System.err.println("Loading inverted index");
        try {
            ObjectInputStream fStream = new ObjectInputStream(new FileInputStream(invIndexFile));
            InvertedIndex invIndex
                    = (InvertedIndex)fStream.readObject();
            fStream.close();
            System.err.println("Inverted index loaded");
            return invIndex;
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
            return null;
        }
    }

    public static void saveIndex(@NotNull final InvertedIndex index, @NotNull final String indexFile) {
        System.err.println("Saving inverted index");
        try {
            ObjectOutputStream fStream = new ObjectOutputStream(new FileOutputStream(indexFile));
            fStream.writeObject(index);
            fStream.close();
            System.err.println("Saved inverted index");
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
        }
    }

    public static void printIndex(@NotNull final InvertedIndex index) {
        final ConcurrentHashMap<String, PostingsList> idx = index.getIndex();
        idx.forEach((term, postings) -> System.out.println(postings.toString()));
    }

    public void resetScoring() {
        documents.forEach((id, doc) -> doc.reset());
    }

    public void resetDatabase(@NotNull final String dbname) {
        try {
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection(String.format("jdbc:sqlite:%s.db", dbname));
            try {
                Statement s = connection.createStatement();
                s.executeUpdate("DROP TABLE POSTINGS && DROP TABLE INDEX");
                s.close();
            } catch (Exception ex) {
            }
            Statement s = connection.createStatement();
            s.executeUpdate("CREATE TABLE POSTINGS " +
                            "(ID             INT PRIMARY KEY  NOT NULL," +
                            " TERM           TEXT    NOT NULL, " +
                            " DOC_FREQUENCY  INT     NOT NULL, " +
                            " POSTINGS_LIST  BINARY  NOT NULL" + 
                            ") && " +
                            "CREATE TABLE INDEX " +
                            "(ID             INT PRIMARY KEY  NOT NULL, " +
                            " DOCID          TEXT NOT NULL, " +
                            " FULLTEXT       TEXT NOT NULL, " +
                            " INDEX_ENTRY    BINARY NOT NULL" +
                            ")");
            s.close();
        } catch ( Exception ex) {
            ex.printStackTrace();
        }
    }

    public void initializeDatabase() {
        try {
        insertPostingsStatement = connection.prepareStatement(
            "INSERT INTO POSTINGS VALUES (?, ?, ?, ?)");
        selectPostingsStatement = connection.prepareStatement(
            "SELECT POSTINGS_LIST FROM POSTINGS WHERE TERM = ?");
        insertIndexDocument = connection.prepareStatement(
            "INSERT INTO INDEX VALUES (?, ?, ?, ?)");
        selectIndexDocument = connection.prepareStatement(
            "SELECT INDEX_ENTRY FROM INDEX WHERE DOCID = ?");
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    public void connectToDatabase(@NotNull final String dbname) {
        try {
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection(String.format("jdbc:sqlite:%s.db", dbname));
        } catch ( Exception ex) {
            ex.printStackTrace();
        }
    }

    public void insertPostings(@NotNull final PostingsList newPosting) {
        try {
            insertPostingsStatement.setInt(1, nextID.incrementAndGet());
            insertPostingsStatement.setString(2, newPosting.term());
            insertPostingsStatement.setInt(3, newPosting.size());
            insertPostingsStatement.setBytes(4, SerializationUtils.serialize(newPosting));
            if (0 == insertPostingsStatement.executeUpdate()) {
                throw new Exception(String.format("failed to insert postings list for term: %s\n", newPosting.term()));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @NotNull
    public PostingsList getPostings(@NotNull final String term) {
        System.out.println("Searching database for postings of term " + term);
        try {
            selectPostingsStatement.setString(1, term);
            ResultSet results = selectPostingsStatement.executeQuery();
            return (PostingsList)SerializationUtils.deserialize(results.getBytes("POSTINGS_LIST"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return new PostingsList(term);
    }
}
