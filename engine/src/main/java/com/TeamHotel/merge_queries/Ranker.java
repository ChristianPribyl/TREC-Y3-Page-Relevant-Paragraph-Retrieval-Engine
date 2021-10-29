package com.TeamHotel.merge_queries;

import com.TeamHotel.inverindex.IndexDocument;
import com.TeamHotel.inverindex.IndexIdentifier;
import com.TeamHotel.inverindex.PostingsList;

import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Collectors;

public class Ranker {
    private final Set<PostingsList> queryPostings;
    private final Map<String, Integer> dfCache;
    private final char[] variant;
    private final int corpusSize;
    private final double[] queryVector;

    public Ranker(String variant, TreeSet<PostingsList> queryPostings, int corpusSize) {
        char[] variantData = variant.toCharArray();
        assert(variantData[0] == variantData[4]);
        assert(variantData.length == 7);

        this.variant = variant.toCharArray();
        this.corpusSize = corpusSize;

        this.queryPostings = queryPostings;

        dfCache = queryPostings.stream().collect(Collectors.toMap(PostingsList::term, PostingsList::size));
        queryVector = calculateQueryVector(queryPostings);
    }

    public static Collection<IndexDocument> tfidf(@NotNull String variant, @NotNull TreeSet<PostingsList> queryTerms,
                                                  @NotNull List<IndexDocument> candidates, int corpusSize) {
        assert(validateVariant(variant));
        Ranker ranker = new Ranker(variant, queryTerms, corpusSize);
        List<IndexDocument> rankings = new LinkedList<>();
        System.out.printf("Ranking %d documents\n", candidates.size());
        candidates.forEach(doc -> {
            double score = ranker.score(doc);
            doc.setFinalScore(score);
            int idx = 0;
            for (IndexDocument d: rankings) {
                if (score > d.getFinalScore()) {
                    break;
                }
                idx++;
            }
            rankings.add(idx, doc);
        });
        return rankings;
    }

    private static boolean validateVariant(@NotNull String variant) {
        /*
        Should support:
        1. lnc.ltn
        2. bnn.bnn
        3. anc.apc
        */
        return (variant.equals("lnc.ltn") || variant.equals("bnn.bnn") || variant.equals("anc.apc"));
    }

    public double score(IndexDocument doc) {
        double[] docVector = calculateDocumentVector(doc);
        return dotProduct(docVector, queryVector);
    }

    private double termFrequencyScore(int TF, int maxTF, char variant) {
        double result = 0;
        switch (variant) {
            case 'n':
                result = TF;
                break;
            case 'l':
                if (TF == 0) result = 1;
                else result = 1 + Math.log(TF);
                break;
            case 'b':
                result = TF > 0? 1 : 0;
                break;
            case 'a':
                if (maxTF == 0) result = 0.5;
                else result = 0.5 + 0.5 * TF / maxTF;
                break;
            default:
                System.err.printf("Unrecognized TF variant: %c\n", variant);
                assert(false);
                return Double.NaN;
        }
        return result;
    }

    private double inverseDocumentFrequencyScore(int DFt, int N, char variant) {
        double result = 0;
        switch (variant) {
            case 'n':
                result = 1;
                break;
            case 't':
                if (DFt == 0 || N == 0) result = 0;
                else result = Math.log(N * 1.0 / DFt);
                break;
            case 'p':
                if (DFt == 0 || N <= DFt) return 0;
                else result = Math.max(0, Math.log((N-DFt) * 1.0/(DFt)));
                break;
            default:
                System.err.printf("Unrecognized IDF variant: %c\n", variant);
                assert(false);
                result = Double.NaN;
        }
        return result;
    }

    private double[] calculateQueryVector(Set<PostingsList> queryPostings) {
        int maxTF = 0;
        for (PostingsList posting : queryPostings) {
            int tf = posting.getQueryTermFrequency();
            if (tf > maxTF) {
                maxTF = tf;
            }
        }

        double[] queryVector = new double[queryPostings.size()];
        int i = 0;
        for (PostingsList postings: queryPostings) {
            int TF = postings.getQueryTermFrequency();
            queryVector[i] = termFrequencyScore(TF, maxTF, variant[4])
                           * inverseDocumentFrequencyScore(dfCache.get(postings.term()), corpusSize, variant[5]);
            i++;
        }

        return normalizeVector(queryVector, variant[6]);
    }

    private double[] calculateDocumentVector(IndexDocument doc) {
        int maxTF = 0;
        Map<String, IndexIdentifier> tfMap = doc.getIDmap();

        int[] tfCache = new int[queryPostings.size()];
        int i = 0;
        for (PostingsList postings: queryPostings) {
            int tf = 0;
            IndexIdentifier id = tfMap.getOrDefault(postings.term(), null);
            if (id != null) {
                tf = id.getTF();
            }
            if (tf > maxTF) {
                maxTF = tf;
            }
            tfCache[i] = tf;
            i++;
        }

        i = 0;
        double[] documentVector = new double[queryPostings.size()];
        for (PostingsList postings: queryPostings) {
            int tf = tfCache[i];
            documentVector[i] = termFrequencyScore(tf, maxTF, variant[0])
                              * inverseDocumentFrequencyScore(dfCache.get(postings.term()), corpusSize, variant[1]);
            i++;
        }

        return normalizeVector(documentVector, variant[2]);
    }

    private double dotProduct(double[] a, double[] b) {
        double sum = 0;
        for (int i = 0; i < a.length && i < b.length; i++) {
            sum += a[i] * b[i];
        }
        return sum;
    }

    private double[] normalizeVector(double[] v, char variant) {
        switch (variant) {
            case 'n':
                return v;
            case 'c':
                double acc = 0;
                for (double value : v) {
                    acc += value * value;
                }
                final double factor =  1 / Math.sqrt(acc);
                for (int i = 0; i < v.length; i++) {
                    v[i] *= factor;
                }
                return v;
            default:
                System.err.printf("Unrecognized vector normalization variant: %c\n", variant);
                assert(false);
                return null;
        }
    }

    public static List<Pair<String, Double>> mergeResults(List<List<Pair<String, Double>>> allFacetResults) {
        return null;
    }
}
