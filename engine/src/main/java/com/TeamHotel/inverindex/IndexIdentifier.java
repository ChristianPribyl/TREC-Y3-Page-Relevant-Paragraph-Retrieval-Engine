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
        int result;
        int idComp = o.trecId.compareTo(trecId);
        int tfComp = Integer.compare(o.termFrequency, termFrequency);
        // if they share an id they are equal
        if (idComp == 0) {
            result = idComp;
        // else rank then by term frequency
        } else if (tfComp != 0) {
            result = tfComp;
        // compare their ids as a tiebreaker
        } else {
            result = idComp;
        }
        //System.out.printf("Comparing (%s/%d and %s/%d resulted in %d\n", trecId, termFrequency, o.trecId, o.termFrequency, result);
        return result;
    }

    /*
    @Override
    public int compareTo(IndexIdentifier o) {
        return o.trecId.compareTo(trecId);
    }
    */
}