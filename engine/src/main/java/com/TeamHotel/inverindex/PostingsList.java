package com.TeamHotel.inverindex;

import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class PostingsList implements Serializable {
    @NotNull private final String term;
    @NotNull private ConcurrentSkipListMap<IndexIdentifier, IndexDocument> postings;
    private int size;
    private int queryTF = 1;

    public PostingsList(@NotNull final String term) {
        this.term = term;
        this.postings = new ConcurrentSkipListMap<>();
        this.size = 0;
    }

    public void add(@NotNull final IndexIdentifier id, @NotNull final IndexDocument doc) {
        if (!postings.containsKey(id)) {
            //System.out.println("Adding not included doument");
            postings.put(id, doc);
            size++;
        } else {
            //System.out.println("Adding already included document.");
        }
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
        if (size > 0) {
            final Iterator<Pair<IndexIdentifier, IndexDocument>>  it = iterator();
            final IndexIdentifier first = it.next().getLeft();
            final StringBuilder s = new StringBuilder(String.format("%s:%d", first.trecId, first.getTF()));
            it.forEachRemaining((Pair<IndexIdentifier, IndexDocument> p) -> {
                IndexIdentifier id = p.getLeft();
                s.append(String.format(",%s:%d", id.trecId, id.getTF()));
            });
            return s.toString();
        } else {
            return "";
        }
    }

    public static PostingsList fromString(@NotNull final String term, @NotNull final String postingString, @NotNull Map<String, IndexDocument> documents) {
        PostingsList postings = new PostingsList(term);
        if (!postingString.isEmpty()) {
            List<String> postingPairs = Arrays.asList(postingString.split(","));
            postingPairs.forEach(p -> {
                final String[] pair = p.split(":");
                final String docId = pair[0];
                final int tf = Integer.parseInt(pair[1]);
                IndexDocument doc = documents.getOrDefault(docId, null);
                if (doc == null) {
                    doc = new IndexDocument(docId);
                    documents.put(docId, doc);
                }
                postings.add(new IndexIdentifier(tf, docId), doc);
            });
        }
        return postings;
    }

    public void truncate(int maxLength) {
        //System.err.printf("Truncating postingslist of length %d ", postings.size());
        ConcurrentSkipListMap<IndexIdentifier, IndexDocument> newPostings = new ConcurrentSkipListMap<>();
        final List<Pair<IndexIdentifier, IndexDocument>> arr = new ArrayList<>();
        postings.forEach((id, doc) -> arr.add(Pair.of(id, doc)));
        assert(postings.size() == arr.size());

        final List<Pair<IndexIdentifier, IndexDocument>> arr2 = arr.subList(0, Math.min(maxLength, arr.size()));
        arr2.forEach(p -> newPostings.put(p.getLeft(), p.getRight()));
        postings = newPostings;
        //System.err.printf("to %d\n", postings.size());
    }

    public void mergeWith(PostingsList that) {
        //System.err.printf("Merging \"%s\"/%d with \"%s\"/%d\n", term(), size(), that.term(), that.size());
        that.postings.forEach((IndexIdentifier id, IndexDocument doc) -> {
            doc.updateRelevantPostings(that.term(), id);
            add(new IndexIdentifier(0, doc.getFullId()), doc); //index doc id is matching too many times
        });
        //System.err.printf("Now \"%s\" has length %d\n", term(), size());
    }
}
