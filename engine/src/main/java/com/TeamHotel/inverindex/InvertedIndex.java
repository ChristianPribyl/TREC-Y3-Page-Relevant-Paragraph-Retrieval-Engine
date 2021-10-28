package com.TeamHotel.inverindex;


import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.TeamHotel.preprocessor.Preprocess;

import org.jetbrains.annotations.NotNull;

public class InvertedIndex implements Serializable {
    private final ConcurrentHashMap<String, PostingsList> index;
    private final ConcurrentHashMap<String, IndexDocument> documents;

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
}
