package com.TeamHotel.inverindex;


import java.io.*;
import java.util.*;

import org.jetbrains.annotations.NotNull;

public class InvertedIndex implements Serializable {
    private final HashMap<String, PostingsList> index;
    private final HashSet<IndexDocument> documents;

    public InvertedIndex(HashMap<String, PostingsList> idx) {
        System.err.println("Initializing in-memory index");
        this.index = idx;
        this.documents = new HashSet<>(idx.size() / 5);
        idx.forEach((term, postings) -> documents.addAll(postings.documents()));
    }

   public HashMap<String, PostingsList> getIndex() {
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

    public static InvertedIndex createInvertedIndex(final String vocabFile, final String cborParagraphs) {
        //make a hashtable chain with linked list 
        // key: vocab, values will be list of docID which the document that has vocab word.
        HashMap<String, PostingsList> invertedIndex = new HashMap<>();
        ArrayList<Index> index = Index.createIndex(cborParagraphs);

        try {
            //Use scanner to read vocab file.
            Scanner vocabScanner = new Scanner(new FileInputStream(vocabFile));

            int indexedWords = 0;
            while (vocabScanner.hasNext())
            {
                String word = vocabScanner.nextLine();
                PostingsList postings = new PostingsList(word);
                index.forEach(idx -> {
                    final Map<String, Integer> terms = idx.getTerms();
                    if (terms.getOrDefault(word, null) != null) {
                        final int tf = idx.getTerms().get(word);
                        postings.add(new IndexIdentifier(tf, idx.carId), idx.getDocument());
                    }
                });
                invertedIndex.put(word, postings);
                indexedWords++;
                if (indexedWords % 1000 == 0) {
                    System.err.printf("indexed %d terms, most recently %s\n", indexedWords, word);
                }
            }
            vocabScanner.close();

        } catch (Exception ex) {
            ex.printStackTrace(System.err);
        }

        return new InvertedIndex(invertedIndex);
    }

    public static InvertedIndex loadInvertedIndex(@NotNull final String invIndexFile) {
        System.err.println("Loading inverted index");
        try {
            ObjectInputStream fStream = new ObjectInputStream(new FileInputStream(invIndexFile));
            HashMap<String, PostingsList> invIndex
                    = (HashMap<String, PostingsList>)fStream.readObject();
            fStream.close();
            System.err.println("Inverted index loaded");
            return new InvertedIndex(invIndex);
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
            return null;
        }
    }

    public static void saveIndex(@NotNull final InvertedIndex index, @NotNull final String indexFile) {
        System.err.println("Saving inverted index");
        final HashMap<String, PostingsList> idx = index.getIndex();
        try {
            ObjectOutputStream fStream = new ObjectOutputStream(new FileOutputStream(indexFile));
            fStream.writeObject(idx);
            fStream.close();
            System.err.println("Saved inverted index");
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
        }
    }

    public static void printIndex(@NotNull final InvertedIndex index) {
        final HashMap<String, PostingsList> idx = index.getIndex();
        idx.forEach((term, postings) -> System.out.println(postings.toString()));
    }

    public void resetScoring() {
        documents.forEach(IndexDocument::reset);
    }
}
