package com.TeamHotel.inverindex;


import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.sql.*;

import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

public class InvertedIndex implements Serializable {
    private final ConcurrentHashMap<String, PostingsList> index;
    private final ConcurrentHashMap<String, IndexDocument> documents;
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

    public void resetScoring() {
        documents.forEach((id, doc) -> doc.reset());
    }

    @NotNull
    public Optional<PostingsList> getPostings(@NotNull final Index idx, @NotNull final String term, @NotNull final Map<String, IndexDocument> documents) {
        return idx.getPostingsList(term, documents);
    }

    public static int generatePostings(@NotNull final Index idx, @NotNull final List<String> vocab, final int offset, final int maxDocuments) {
        Iterator<Pair<String, Map<String, Integer>>> docIterator = idx.getDocumentTermFrequencies(offset, maxDocuments);
        int numDocs = 0;
        int stepSize = 100000;
        final int maxPostingsSize = 100000;
        while (docIterator.hasNext()) {
            System.err.println("Reading more documents");
            Map<String, ArrayList<Pair<String, Integer>>> postings = new HashMap<>(vocab.size() * 2);
            vocab.forEach(word -> postings.put(word, new ArrayList<>()));
            System.gc();
            for (int i = 0; i < 1000000 & docIterator.hasNext(); i++) {
                final Pair<String, Map<String, Integer>> p = docIterator.next();
                final String docId = p.getLeft();
                final Map<String, Integer> terms = p.getRight();
                vocab.forEach(term -> {
                    final int tf = terms.getOrDefault(term, 0);
                    if (tf > 0) {
                        postings.get(term).add(Pair.of(docId, tf));
                    }
                });
                numDocs++;
                if (numDocs % stepSize == 0) {
                    System.err.printf("Parsed %d documents\n", numDocs);
                }
            }
            System.err.printf("Updating postings lists after parsing %d documents\n", numDocs);
            idx.beginTransaction();
            postings.forEach((term, postingsList) -> {
                if (!postingsList.isEmpty()) {
                    idx.updatePostingsList(term, postingsList, maxPostingsSize);
                }
            });
            idx.commitTransaction();
        }

        return 0;
    }
}
