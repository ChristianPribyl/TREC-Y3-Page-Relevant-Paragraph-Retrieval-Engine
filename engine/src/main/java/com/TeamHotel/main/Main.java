package com.TeamHotel.main;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.TeamHotel.inverindex.Index;
import com.TeamHotel.inverindex.InvertedIndex;
import com.TeamHotel.merge_queries.Merge_Queries;
import com.TeamHotel.merge_queries.Ranker;
import com.TeamHotel.preprocessor.Preprocess;

import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

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
            case "query-vocab": {
                if (args.length == 3) {
                    final String cborQueryFile = args[1];
                    final String outfile = args[2];
                    final Map<String, List<List<String>>> queries = Preprocess.preprocessFacetedQueries(cborQueryFile);
                    final Set<String> vocab = Preprocess.getFacetedQueryVocabulary(queries);
                    System.err.printf("Determined vocabulary contains %d terms\n", vocab.size());
                    try {
                        final FileWriter outf = new FileWriter(outfile);
                        vocab.stream().sorted().forEach(w -> {
                            try {
                                outf.write(String.format("%s\n", w));
                            } catch (IOException ex) {
                                ex.printStackTrace();
                            }
                        });
                        outf.close();
                    } catch (IOException ex) {
                        ex.printStackTrace();
                        System.err.println("Failed to write vocab to file");
                    }
                } else {
                    System.err.println("Usage: ir-engine query-vocab <cbor-queries> <outfile>");
                }
                break;
            }
            case "corpus-vocab": {
                if (args.length == 5) {
                    final String cborCorpus = args[1];
                    final String outFile = args[2];
                    final int offset = Integer.parseInt(args[3]);
                    final int maxDocuments = Integer.parseInt(args[4]);
                    System.err.println("Determining corpus vocabulary");
                    Set<String> vocab = Preprocess.extractCorpusVocab(cborCorpus, offset, maxDocuments);
                    System.err.printf("Determined vocabulary contains %d terms\n", vocab.size());
                    try {
                        final FileWriter outf = new FileWriter(outFile);
                        vocab.forEach(w -> {
                            try {
                                outf.write(String.format("%s\n", w));
                            } catch (IOException ex) {
                                ex.printStackTrace();
                            }
                        });
                        System.err.println("Wrote vocabulary to file");         
                    } catch (IOException ex) {
                        ex.printStackTrace();
                        System.err.println("Failed to write vocab to file");
                    }           
                } else {
                    System.err.println("Usage: ir-engine corpus-vocab <cbor-paragraphs> <outfile> <offset> <max-paragraphs-to-parse>");
                }
                break;
            }
            case "preprocess-similarity-vectors": {
                if (args.length == 3) {
                    final String vectorFile = args[1];
                    final String outFile = args[2];
                    System.err.println("Preprocessing Word Vectors");
                    @NotNull final Scanner inScanner = new Scanner(new FileInputStream(vectorFile));
                    @NotNull final FileWriter outf = new FileWriter(outFile);
                    int numProcessed = WordSimilarity.preprocessWordVectors(inScanner, outf);
                    inScanner.close();
                    outf.close();
                    System.out.printf("Processed %d word vectors\n", numProcessed);
                } else {
                    System.out.println("Usage: ir-engine preprocess-similarity-vectors <infile> <outfile>");
                }
                break;
            }
            case "make-postings": {
                if (args.length == 5) {
                    final String dbname = args[1];
                    final String vocabFile = args[2];
                    final int offset = Integer.parseInt(args[3]);
                    final int maxDocuments = Integer.parseInt(args[4]);
                    //final int maxLength = Integer.parseInt(args[3]);
                    System.out.println("Loading vocab");
                    final List<String> vocab = Preprocess.loadVocab(vocabFile);
                    System.err.println("Instantiating Index");
                    Index idx = Index.load(dbname).get();
                    System.err.println("Generating postings lists");
                    InvertedIndex.generatePostings(idx, vocab, offset, maxDocuments);
                } else {
                    System.out.println("Usage: ir-engine make-postings <index> <vocab-file> <offset> <max-documents-to-parse>");
                }
                break;
            }
            case "reset-postings": {
                if (args.length == 3) {
                    final String dbname = args[1];
                    final String vocabFile = args[2];
                    System.out.println("Loading vocab");
                    final List<String> vocab = Preprocess.loadVocab(vocabFile);
                    System.err.println("Instantiating Index");
                    Index idx = Index.load(dbname).get();
                    idx.deletePostings();
                    idx.addEmptyPostings(vocab);
                } else {
                    System.out.println("Usage: ir-engine reset-postings <index> <vocab-file>");
                }
                break;
            }
            case "create-empty-index": {
                if (args.length == 2) {
                    final String dbname = args[1];
                    System.err.println("Creating new empty index");
                    Optional<Index> idx = Index.createNew(dbname);
                    if (idx.isEmpty()) {
                        System.err.println("Index creation failed");
                    } else {
                        System.err.println("Index creation succeeded");
                    }
                } else {
                    System.err.println("Usage: ir-engine create-empty-index <index-location>");
                }
                break;
            }
            case "add-documents": {
                if (args.length == 6) {
                    final String dbname = args[1];
                    final String vocabFile = args[2];
                    final String cborCorpus = args[3];
                    final int offset = Integer.parseInt(args[4]);
                    final int maxDocuments = Integer.parseInt(args[5]);
                    System.out.println("Loading vocab");
                    final List<String> vocab = Preprocess.loadVocab(vocabFile);
                    System.err.println("Instantiating Index");
                    Index idx = Index.load(dbname).get();
                    System.err.println("Adding documents to index");
                    int numAdded = idx.addNewDocuments(cborCorpus, vocab.stream().collect(Collectors.toSet()), offset, maxDocuments);
                    System.err.printf("Added %d documents to the Index\n", numAdded);
                } else {
                    System.err.println("Usage: ir-engine add-documents <index-location> <vocab-file> <corpus-file> <corpus-offset> <max-to-add>");
                }
                break;
            }
            case "calculate-vectors": {
                if (args.length == 5) {
                    final String dbname = args[1];
                    final String wordVectorFile = args[2];
                    final int offset = Integer.parseInt(args[3]);
                    final int maxDocuments = Integer.parseInt(args[4]);
                    System.err.println("Instantiating Index");
                    Index idx = Index.load(dbname).get();
                    System.err.println("Calculating document vectors");
                    int numCalculated = WordSimilarity.calculateDocVectors(idx, wordVectorFile, offset, maxDocuments);
                    System.err.printf("Calculated vectors for %d documents\n", numCalculated);
                } else {
                    System.err.println("Usage: ir-engine calculate-vectors <index-location> <word-vector-file> <corpus-offset> <max-to-process>");
                }
                break;
            }
            case "cluster": {
                if (args.length == 5) {
                    final String dbname = args[1];
                    final int numClusterPasses = Integer.parseInt(args[2]);
                    final int offset = Integer.parseInt(args[3]);
                    final int maxDocuments = Integer.parseInt(args[4]);
                    System.err.println("Instantiating Index");
                    Index idx = Index.load(dbname).get();
                    System.err.println("Clustering documents");
                    int numClusters = Clustering.clusterDocuments(idx, numClusterPasses, offset, maxDocuments);
                    System.err.printf("Clustered %d documents\n", numClusters);
                } else {
                    System.err.println("Usage: ir-engine ccluster <index-location> <num-cluster-passes> <corpus-offset> <max-docs-to-cluster>");
                }
                break;
            }
            case "cbor-query": {
                if (args.length == 5) {
                    final String invertedIndexFile = args[1];
                    final String cborQueryFile = args[2];
                    final String mergeType = args[3].toUpperCase();
                    assert (mergeType.equals("AND") || mergeType.equals("OR"));
                    final String tfidfVariant = args[4];
                    // preprocess cbor queries.
                    // execute queries in sequence.
                    final InvertedIndex invertedIndex = null;//InvertedIndex.loadInvertedIndex(invertedIndexFile);
                    assert invertedIndex != null;
                    final ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> queries = Preprocess.preprocessCborQueries(cborQueryFile, Preprocess.QueryType.PAGES);
                    StringBuilder out = new StringBuilder();
                    queries.forEach((id, terms) -> {
                        System.out.println((String.format("Processing query %s\n", id)));
                        terms.forEach((term, num) -> System.out.printf("%s ", term));
                        System.out.println();
                        //Merge_Queries.query(invertedIndex, terms, mergeType, tfidfVariant).forEach(r -> out.append(String.format("%s Q0 %s\n", id, r)));
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
                    Map<String, List<List<String>>> facetedQueries = Preprocess.preprocessFacetedQueries(cborQueryFile);

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
            case "tfidf-cbor-query": {
                if (args.length == 5) {
                    final String dbname = args[1];
                    final String cborQueryFile = args[2];
                    //final String mergeType = args[3].toUpperCase();
                    //assert (mergeType.equals("AND") || mergeType.equals("OR"));
                    final String tfidfVariant = args[4];
                    // preprocess cbor queries.
                    // execute queries in sequence.
                    final Index idx = Index.load(dbname).get();
                    final Map<String, List<List<String>>> facetedQueries = Preprocess.preprocessFacetedQueries(cborQueryFile);

                    FileWriter logfile = new FileWriter("../queryLog.txt");
                    FileWriter runFile = new FileWriter("../tfidf-atc_btc-TeamHotel.run");

                    Map<String, List<List<Pair<String, Double>>>> queryResults = new TreeMap<>();

                    //Map<String, List<List<String>>> lessQueries = new HashMap<>(5);
                    //facetedQueries.entrySet().stream().collect(Collectors.toList()).subList(0, 1).forEach(e -> lessQueries.put(e.getKey(), e.getValue()));
                    //lessQueries.forEach((queryId, facets) -> {
                    facetedQueries.forEach((queryId, facets) -> {
                        queryResults.put(queryId, new ArrayList<>(facets.size()));
                        facets.forEach(facet -> {
                            Map<String, Integer> terms = new HashMap<>();
                            facet.stream().forEach((String t) -> {
                                Integer prev = terms.put(t, 1);
                                if (prev != null) {
                                    terms.put(t, prev + 1);
                                }
                            });
                            try {
                                logfile.write(String.format("Querying Facet of %s with terms: ", queryId));
                                facet.forEach(w -> {
                                    try {
                                        logfile.write(String.format(" %s", w));
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                });
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            System.out.println();
                            List<Pair<String, Double>> facetResults = Merge_Queries.query(idx, terms, "OR", tfidfVariant, logfile);
                            queryResults.get(queryId).add(facetResults);
                            System.out.printf("Facet has %d documents\n", facetResults.size());
                            System.out.printf("Query %s has %d facet results\n", queryId, queryResults.get(queryId).size());
                        });
                    });

                    final String modelName = "tfidf_atc.btc";
                    final String teamName = "TeamHotel";

                    Map<String, List<Pair<String, Double>>> finalResults = new HashMap<>(queryResults.size() * 2);
                    queryResults.forEach((qid, facets) -> {
                        System.err.printf("qid %s has %d facets\n", qid, facets.size());
                        List<Pair<String, Double>> result = Merge_Queries.mergeFacets(facets);
                        System.err.printf("merging them together we get %d ranked documents\n", result.size());
                        finalResults.put(qid, result);
                    });

                    System.err.printf("Final results contains results for %d queries\n", finalResults.size());

                    AtomicInteger i = new AtomicInteger(1);
                    finalResults.forEach((String qid, List<Pair<String, Double>> results) -> {
                        i.set(1);
                        results.forEach((Pair<String, Double> p) -> {
                            final String docid = p.getLeft();
                            final Double score = p.getRight();
                            try {
                                runFile.write(String.format("%s Q0 %s %d %f %s-%s\n", qid, docid, i.getAndIncrement(), score, teamName, modelName));
                            } catch (IOException ex) {
                                ex.printStackTrace();
                            }
                            //$queryId Q0 $paragraphId $rank $score $teamName-$methodName
                        });
                    });
                    runFile.close();
                } else {
                    cborQueryUsage();
                }
                break;
            }
            case "query-ids": {
                if (args.length == 2) {
                    final String queryFile = args[1];
                    Preprocess.dumpQueryIds(queryFile);
                } else {
                    System.err.println("Usage: ir-engine <query-ids>");
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

    static void usage() {
        System.out.println("USage: ir-engine [query-vocab | corpus-vocab | preprocess-similarity-vectors\n | create-empty-index | add-documents | calculate-vectors\n | cluster | cluster-cbor-query | make-postings | reset-postings]\n" +
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
