package com.TeamHotel.main;

import com.TeamHotel.preprocessor.Preprocess;

import org.apache.commons.lang3.tuple.Pair;

import com.TeamHotel.inverindex.*;

import com.TeamHotel.merge_queries.Merge_Queries;
import com.TeamHotel.merge_queries.Ranker;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.io.FileWriter;
import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        System.setProperty("file.encoding", "UTF-8");
        if (args.length == 0) {
            usage();
            return;
        }
        switch (args[0]) {
            case "vocab":
                Set<String> vocabulary;
                if (args.length == 3 && args[1].equals("queries")) {
                    // args[2] cbor-outlines file

                    final String cborQueryFile = args[2];
                    // Map<QueryId, Set<Unique words>>
                    final ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> queries = Preprocess.preprocessCborQueries(cborQueryFile, Preprocess.QueryType.PAGES);
                    vocabulary = Preprocess.getVocabulary(queries);
                } else if (args.length == 2) {
                    // args[1] cbor-paragraphs file

                    final String cborParagraphsFile = args[1];
                    // Map<paragraphsId, Map<term, tf>>
                    final ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> paragraphs = Preprocess.preprocessLargeCborParagrphsWithVocab(cborParagraphsFile, Collections.<String>emptySet(), 0, 2500000);
                    vocabulary = Preprocess.getVocabulary(paragraphs);
                } else {
                    vocabUsage();
                    return;
                }
                Preprocess.printVocabulary(vocabulary);
                break;
            case "index": {
                if (args.length == 4) {
                    // args[1]: vocab
                    // args[2]: cborParagraphs
                    // args[3]: index save location
                    final InvertedIndex invertedIndex = InvertedIndex.createInvertedIndex(args[1], args[2], 0, 100000000);
                    InvertedIndex.saveIndex(invertedIndex, args[3]);
                                        /*
                    new java.io.File(args[3]).mkdir();
                    InvertedIndex invertedIndex = InvertedIndex.createInvertedIndex(args[1], args[2], 0, 4000000);
                    InvertedIndex.saveIndex(invertedIndex, args[3] + "/1.dat");
                    
                    // The inverted index takes a lot of space.  We want the garbage collector to free 
                    // the previous one before we start creating another.  Java can't discard it until all
                    // references are removed.
                    invertedIndex = null;
                    System.gc(); 

                    invertedIndex = InvertedIndex.createInvertedIndex(args[1], args[2], 4000000, 4000000);
                    InvertedIndex.saveIndex(invertedIndex, args[3] + "/2.dat");
                    invertedIndex = null;
                    System.gc();
                    invertedIndex = InvertedIndex.createInvertedIndex(args[1], args[2], 8000000, 4000000);
                    InvertedIndex.saveIndex(invertedIndex, args[3] + "/3.dat");
                    invertedIndex = null;
                    System.gc();
                    invertedIndex = InvertedIndex.createInvertedIndex(args[1], args[2], 12000000, 4000000);
                    InvertedIndex.saveIndex(invertedIndex, args[3] + "/4.dat");
                    invertedIndex = null;
                    System.gc();
                    invertedIndex = InvertedIndex.createInvertedIndex(args[1], args[2], 16000000, 4000000);
                    InvertedIndex.saveIndex(invertedIndex, args[3] + "/5.dat");
                    invertedIndex = null;
                    System.gc();
                    */
                } else {
                    indexUsage();
                }
                break;
            }
            case "index-db": {
                if (args.length == 4) {
                    final String vocabFile = args[1];
                    final String dbname = args[2];
                    final String cborCorpus = args[3];
                    final int maxClusteringRepititions = 100;
                    final int offset = 0; // number of documents to skip

                    List<String> vocab = Preprocess.loadVocab(vocabFile);

                    Index idx = Index.createNew(dbname).get();
                        
                    idx.addNewDocuments(cborCorpus, vocab.stream().collect(Collectors.toSet())); // INDEX(DOCID, FULLTEXT, PREPROCESSED_TOKENS, TOKEN_SET, CLASS="", VECTOR=Array_all_0, LEADER=FALSE, FAKE=FALSE)
                    WordSimilarity.calculateWordVectors(idx, offset);
                    Clustering.clusterDocuments(idx, maxClusteringRepititions);
                } else {
                    dumpIndexDbUsage();
                }
                break;
            }
            case "dump-index":
                if (args.length == 2) {
                    // args[1] inverted index
                    InvertedIndex.printIndex(Objects.requireNonNull(InvertedIndex.loadInvertedIndex(args[1])));
                } else {
                    dumpIndexUsage();
                }
                break;
            case "query":
                /* performs merge query of inverted index
                 * 	will prompt user to  choose whether to do an 'AND' or 'OR' merge query
                 * 	OR merge query will be implemented later
                 *
                 * args[1] inverted index
                 * args[2] AND | OR (merge type)
                 * args[3] tfidf variant (ddd.qqq)
                 */
                if (args.length == 4) {
                    InvertedIndex index = Objects.requireNonNull(InvertedIndex.loadInvertedIndex(args[1]));
                    Scanner query_input = new Scanner(System.in);
                    while (Merge_Queries.merge_inverted_index(index, args[2], args[3], query_input));
                } else {
                    queryUsage();
                }
                break;
            case "cbor-query": {
                if (args.length == 5) {
                    final String invertedIndexFile = args[1];
                    final String cborQueryFile = args[2];
                    final String mergeType = args[3].toUpperCase();
                    assert (mergeType.equals("AND") || mergeType.equals("OR"));
                    final String tfidfVariant = args[4];
                    // preprocess cbor queries.
                    // execute queries in sequence.
                    final InvertedIndex invertedIndex = InvertedIndex.loadInvertedIndex(invertedIndexFile);
                    assert invertedIndex != null;
                    final ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> queries = Preprocess.preprocessCborQueries(cborQueryFile, Preprocess.QueryType.PAGES);
                    StringBuilder out = new StringBuilder();
                    queries.forEach((id, terms) -> {
                        System.out.println((String.format("Processing query %s\n", id)));
                        terms.forEach((term, num) -> System.out.printf("%s ", term));
                        System.out.println();
                        Merge_Queries.query(invertedIndex, terms, mergeType, tfidfVariant).forEach(r -> out.append(String.format("%s Q0 %s\n", id, r)));
                        // $queryId Q0 $paragraphId $rank $score $teamName-$methodName
                    });
                    try {
                        FileWriter outFile = new FileWriter(String.format("%s" + "-Y3.run", mergeType+tfidfVariant));
                        outFile.write(out.toString());
                        outFile.close();
                    } catch (IOException ex) {
                        ex.printStackTrace(System.err);
                        System.err.println("Failed to write results to run file");
                    }
                } else {
                    cborQueryUsage();
                }
                break;
            }
            case "cluster-cbor-query": {
                if (args.length == 3) {
                    final String dbname = args[1];
                    final String cborQueryFile = args[2];
                    final int resultsPerQuery = 20;

                    // generate list of queries
                    // facetedQueries = Map<queryid, List<queryFacets>>
                    // queryFacet = List<queryTerms>
                    //
                    // Each query facet should be treated like a normal query.
                    // For each query id, we query each individual facet query, and merge the results.
                    Map<String, List<List<String>>> facetedQueries = Preprocess.readFacetedQueries(cborQueryFile);

                    Index idx = Index.load(dbname).get();

                    FileWriter outFile = new FileWriter(String.format("WordSimilarity+Clustering-Y3.run"));

                    facetedQueries.forEach((queryID, facets) -> {
                        final List<List<Pair<String, Double>>> allFacetResults = new LinkedList<>();
                        facets.forEach((List<String> queryFacet) -> {
                            // List<DocID, Score>
                            final List<Pair<String, Double>> facetResults = Clustering.query(idx, queryFacet, resultsPerQuery);
                            allFacetResults.add(facetResults);
                        });
                        final List<Pair<String, Double>> finalResult = Ranker.mergeResults(allFacetResults);

                        AtomicInteger i = new AtomicInteger();
                        finalResult.forEach(p -> {
                            try {
                                outFile.write(String.format("%s Q0 %s %d %f TeamHotel-%s", queryID, p.getLeft(), i, p.getRight(), "WordSimilarity"));
                            } catch (IOException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                            idx.logResult(queryID, p.getLeft(), p.getRight(), i.getAndIncrement());
                        });

                    });
                } else {
                    dumpClusterCborQueryUsage();
                }
                break;
            }
            default:
                usage();
        }
    }

    private static void dumpClusterCborQueryUsage() {
        System.out.println("Usage: ir-engine cluster-cbor-query index-name cborQueryFile");
    }

    private static void dumpIndexDbUsage() {
        System.out.println("Usage: ir-engine index-db vocabFile index-name cborParagraphs");
    }

    static void usage() {
        System.out.println("USage: ir-engine [vocab | index-db | cluster-cbor-query]\n" +
        "Run commands for specific usage instructions");
        //System.out.println("Usage: prog-4 [vocab | index | dump-index | query | cbor-query]\n" +
        //        "Run commands for specific usage instructions");
    }

    static void vocabUsage() {
        System.out.println("Usage: prog-4 vocab <cbor-paragraphs>\n" +
                "       prog-4 queries <cbor-outlines>");
    }

    static void indexUsage() {
        System.out.println("Usage: prog-4 index <vocab-file> <cbor-paragraphs-file> <index-save-location>");
    }

    static void dumpIndexUsage() {
        System.out.println("Usage: prog-4 dump-index <index-file>");
    }

    static void queryUsage() {
        System.out.println("Usage: prog-4 query <index-file> [AND | OR] <qqq.ddd>");
    }

   static void cborQueryUsage() {
        System.out.println("Usage: prog-4 cbor-query <index-file> <cbor-query-file> [AND | OR] <qqq.ddd>");
    }
}
