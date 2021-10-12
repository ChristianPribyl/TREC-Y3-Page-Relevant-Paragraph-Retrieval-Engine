package com.TeamHotel.inverindex;

import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class PostingsList implements Serializable {
    private final String term;
    private final ConcurrentSkipListMap<IndexIdentifier, IndexDocument> postings;
    private int size;
    private int queryTF = 1;

    public PostingsList(@NotNull String term) {
        this.term = term;
        this.postings = new ConcurrentSkipListMap<>();
        this.size = 0;
    }

    public void add(IndexIdentifier id, IndexDocument doc) {
        assert(!postings.containsKey(id));
        postings.put(id, doc);
        size++;
    }

    public void incQueryTermFrequency() {
        queryTF++;
    }

    public int getQueryTermFrequency() {
        return queryTF;
    }

    public String term() {
        return term;
    }

    public List<IndexDocument> documents() {
        return new ArrayList<>(postings.values());
    }

    public int size() {
        return size;
    }

    public Iterator<Pair<IndexIdentifier, IndexDocument>> iterator() {
        ArrayList<Pair<IndexIdentifier, IndexDocument>> arr = new ArrayList<>();
        postings.forEach((id, doc) -> arr.add(Pair.of(id, doc)));
        return arr.iterator();
    }

    public String toString() {
        StringBuilder res = new StringBuilder(String.format("Postings for %s (doc frequency: %d)\n", term(), size()));
        postings.forEach((id, doc) -> res.append(String.format("tf: %d, id: %s\n", id.termFrequency, doc.getFullId())));
        return res.toString();
    }
}
