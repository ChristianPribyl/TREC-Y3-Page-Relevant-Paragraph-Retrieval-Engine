package com.TeamHotel.inverindex;

import org.jetbrains.annotations.NotNull;

import java.io.Serializable;

// Meta-data stored in each posting alongside the indexed document
public class IndexIdentifier implements Serializable, Comparable<IndexIdentifier> {
    int termFrequency;
    String trecId;
    boolean fake;

    public IndexIdentifier(int termFrequency, @NotNull String trecId) {
        this.termFrequency = termFrequency;
        this.trecId = trecId;
        fake = false;
    }

    private IndexIdentifier() {
        termFrequency = 0;
        trecId = null;
    }
    public static IndexIdentifier placeholder(@NotNull String trecId) {
        IndexIdentifier id = new IndexIdentifier();
        id.fake = true;
        id.trecId = trecId;
        return id;
    }

    public boolean fake() {
        return fake;
    }

    public int getTF() {
        return termFrequency;
    }

    @Override
    public int compareTo(IndexIdentifier o) {
        return o.trecId.compareTo(trecId);
    }
}