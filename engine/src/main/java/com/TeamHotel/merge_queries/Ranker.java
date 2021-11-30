package com.TeamHotel.merge_queries;

import com.TeamHotel.inverindex.IndexDocument;
import com.TeamHotel.inverindex.IndexIdentifier;
import com.TeamHotel.inverindex.PostingsList;

import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Collectors;

public class Ranker {
    private final Set<PostingsList> queryPostings;
    private final Map<String, Integer> dfCache;
    private char[] variant;
    private final int corpusSize;
    private double[] queryVector;
    private double k1;
    private double k3;
    private double beta;

    public Ranker(TreeSet<PostingsList> queryPostings, int corpusSize) {
        this.corpusSize = corpusSize;
        this.queryPostings = queryPostings;
        dfCache = queryPostings.stream().collect(Collectors.toMap(PostingsList::term, PostingsList::size));
    }

    private static Ranker tfidfRanker(String variant, TreeSet<PostingsList> queryPostings, int corpusSize) {
        Ranker r = new Ranker(queryPostings, corpusSize);
        char[] variantData = variant.toCharArray();
        assert(variantData[0] == variantData[4]);
        assert(variantData.length == 7);
        r.variant = variantData;
        r.queryVector = r.calculateQueryVector(queryPostings);
        return r;
    }

    public static Collection<IndexDocument> tfidf(@NotNull String variant, @NotNull TreeSet<PostingsList> queryTerms,
                                                  @NotNull List<IndexDocument> candidates, int corpusSize) {
        assert(validateVariant(variant));
        Ranker ranker = tfidfRanker(variant, queryTerms, corpusSize);
        List<IndexDocument> rankings = new LinkedList<>();
        //System.out.printf("Ranking %d documents\n", candidates.size());
        candidates.forEach(doc -> {
            double score = ranker.tfidfScore(doc);
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

    private static Ranker bm25Ranker(TreeSet<PostingsList> queryTerms, int corpusSize, double k1, double k3, double beta) {
        Ranker r = new Ranker(queryTerms, corpusSize);
        r.k1 = k1;
        r.k3 = k3;
        r.beta = beta;
        return r;
    }

    private static Ranker jelinekMercerRanker(TreeSet<PostingsList> queryTerms, int corpusSize, double beta) {
        Ranker r = new Ranker(queryTerms, corpusSize);
        r.beta = beta;
        return r;
    }

    public static Collection<IndexDocument> bm25(final @NotNull TreeSet<PostingsList> queryTerms, final @NotNull List<IndexDocument> candidates,
            int corpusSize, double k1, double k3, double beta) {
        Ranker ranker = bm25Ranker(queryTerms, corpusSize, k1, k3, beta);
        List<IndexDocument> rankings = new LinkedList<>();
        //System.out.printf("Ranking %d documents\n", candidates.size());
        candidates.forEach(doc -> {
            double score = ranker.bm25Score(doc);
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

    public static Collection<IndexDocument> jelinekMercer(final @NotNull TreeSet<PostingsList> queryTerms, final @NotNull List<IndexDocument> candidates,
                                                 int corpusSize, double beta) {
        Ranker ranker = jelinekMercerRanker(queryTerms, corpusSize, beta);
        List<IndexDocument> rankings = new LinkedList<>();
        //System.out.printf("Ranking %d documents\n", candidates.size());
        candidates.forEach(doc -> {
            double score = ranker.jelinekMercerScore(doc);
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

    private double jelinekMercerScore(IndexDocument doc) {
        return queryPostings.stream().collect(Collectors.summarizingDouble((PostingsList termPosting) -> {
            double tfd = doc.termFrequency(termPosting.term());
            //Unsure if this is the term's frequency out of every document in corpus or not
            double tfq = termPosting.getQueryTermFrequency();
            double pt = tfq / corpusSize;
            return ( Math.log(  ( beta * ( ( tfd ) / (doc.getNumTerms() - tfd ) ) ) + (1-beta) * pt ) );
        })).getSum();
    }

    private double bm25Score(IndexDocument doc) {
        double L = (1 - beta) + (beta * doc.getNumTerms());
        return queryPostings.stream().collect(Collectors.summarizingDouble((PostingsList termPosting) -> {
            double tfd = doc.termFrequency(termPosting.term());
            double tfq = termPosting.getQueryTermFrequency();
            return (Math.log(corpusSize * 1.0 / termPosting.size())
            * ((k1 + 1) * tfd)
            * ((k3 + 1) * tfq)
            / (((k1 * L) + tfd)
              * (k3 + tfq)));
        })).getSum();
    }

    public static Collection<IndexDocument> bim(final @NotNull TreeSet<PostingsList> queryTerms, final @NotNull List<IndexDocument> candidates,
    int corpusSize) {
        Ranker r = new Ranker(queryTerms, corpusSize);
        List<IndexDocument> rankings = new LinkedList<>();
        //System.out.printf("Ranking %d documents\n", candidates.size());
        candidates.forEach(doc -> {
            double score = r.bimScore(doc);
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
    private double bimScore(IndexDocument doc) {
        return queryPostings.stream().collect(Collectors.summarizingDouble((PostingsList termPosting) -> {
            double N = corpusSize + 2;
            double ct = 0.0;
            if (doc.termFrequency(termPosting.term()) > 0.0) {
                double nt = termPosting.size() + 1;
                ct = calCT(N,nt);
            }
            return ct;

        })).getSum();
        
        
    }

    public static double calCT(double N, double nt)
    {
        // ct: log (pt + lap) - log (1-pt + lap) - log(nt - pt + lap) + log(N - nt - 1 + pt + lap) from book 
        double ct = 0.0;
        //ct = Math.log(pt + lapace) + Math.log((1-ut) + lapace) - Math.log(ut+lapace) - Math.log((1-pt)+ lapace);
        //ct = Math.log(pt + lapace) - Math.log((1-pt) + lapace) - Math.log(nt - pt + lapace) + Math.log((N - nt - 1 + pt) + lapace);
        //ct = Math.log((((pt * (1-ut) ) + 1) / ((ut * (1-pt)) + lapace) ));
        ct = Math.log((N - nt)/ nt);
        return ct;
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

    private double tfidfScore(IndexDocument doc) {
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
}
