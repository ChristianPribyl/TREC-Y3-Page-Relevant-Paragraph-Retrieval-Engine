package com.TeamHotel.inverindex;

import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.lang.Comparable;
import java.util.Map;
import java.util.TreeMap;

// Represents an indexed document that is shared between multiple postings lists
public class IndexDocument implements Comparable<IndexDocument>, Serializable {
    static int nextId = 0;
    final String id;
    final int internalId;
    String fulltext;
    double quality;
    int numHits;
    double finalScore = 1.0;
    TreeMap<String, IndexIdentifier> relevantPostings;

    public IndexDocument(final String id) {
        this.id = id;
        this.internalId = nextId;
        nextId++;
        quality = 1.0;
        numHits = 0;
        relevantPostings = new TreeMap<>();
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
}
