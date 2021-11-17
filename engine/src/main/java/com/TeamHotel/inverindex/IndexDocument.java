package com.TeamHotel.inverindex;

import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.lang.Comparable;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.BinaryOperator;

// Represents an indexed document that is shared between multiple postings lists
public class IndexDocument implements Comparable<IndexDocument>, Serializable {
    public static Index index = null;
    static int nextId = 0;
    final String id;
    final int internalId;
    String fulltext;
    double quality;
    int numHits;
    double finalScore = 1.0;
    TreeMap<String, IndexIdentifier> relevantPostings;
    Map<String, Integer> termFrequencies;
    Optional<Integer> numTerms;

    public IndexDocument(final String id) {
        this.id = id;
        this.internalId = nextId;
        nextId++;
        quality = 1.0;
        numHits = 0;
        relevantPostings = new TreeMap<>();
        termFrequencies = null;
        numTerms = Optional.empty();
    }

    public void setFulltext(String fulltext) {
        this.fulltext = fulltext;
    }

    public void setQuality(double quality) {
        this.quality = quality;
    }

    public String getFulltext() {
        return fulltext;
    }

    public double getInternalId() {
        return internalId;
    }

    public String getFullId() {
        return id;
    }

    public void updateRelevantPostings(@NotNull String term, @NotNull IndexIdentifier id) {
        if (!id.fake()) {
            relevantPostings.put(term, id);
        }
    }

    public void reset() {
        relevantPostings = new TreeMap<>();
        finalScore = 1.0;
    }

    @Override
    public int compareTo(@NotNull IndexDocument o) {
        return o.getInternalId() >= getInternalId()? -1 : 0;
    }

    public Map<String, IndexIdentifier> getIDmap() {
        return relevantPostings;
    }

    public void setFinalScore(double score) {
        finalScore = score;
    }

    public double getFinalScore() {
        return finalScore;
    }

    public int getNumTerms() {
        if (termFrequencies == null) {
            Optional<Map<String, Integer>> r = index.getDocumentTermMap(id);
            if (r.isPresent()) {
                termFrequencies = r.get();
            } else {
                return 0;
            }
        }

        if (numTerms.isEmpty()) {
            numTerms = termFrequencies.values().stream().reduce(new BinaryOperator<Integer>() {
                @Override
                public Integer apply(Integer t, Integer u) {
                    return t + u;
                } 
            });
        }
        return numTerms.get();
    }

    public double termFrequency(String term) {
        if (termFrequencies == null) {
            Optional<Map<String, Integer>> r = index.getDocumentTermMap(id);
            if (r.isPresent()) {
                termFrequencies = r.get();
            } else {
                return 0;
            }
        }

        return termFrequencies.getOrDefault(term, 0);
    }
}
