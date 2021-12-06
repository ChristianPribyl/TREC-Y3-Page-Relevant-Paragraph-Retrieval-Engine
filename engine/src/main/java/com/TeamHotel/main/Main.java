package com.TeamHotel.main;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.TeamHotel.inverindex.Index;
import com.TeamHotel.inverindex.InvertedIndex;
import com.TeamHotel.merge_queries.Merge_Queries;
import com.TeamHotel.preprocessor.Preprocess;

import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

public class Main {
    static final String teamName = "TeamHotel";
    static final String progName = "ir-engine";
    public static void main(String[] args) throws IOException {
        System.setProperty("file.encoding", "UTF-8");
        if (args.length == 0) {
            return;
        }
        switch (args[0]) {
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
                    System.err.printf("Usage: %s query-vocab <cbor-queries> <outfile>\n", progName);
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
                        outf.close();
                        System.err.println("Wrote vocabulary to file");         
                    } catch (IOException ex) {
                        ex.printStackTrace();
                        System.err.println("Failed to write vocab to file");
                    }           
                } else {
                    System.err.printf("Usage: %s corpus-vocab <cbor-paragraphs> <outfile> <offset> <max-paragraphs-to-parse>\n", progName);
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
                    System.out.printf("Usage: %s preprocess-similarity-vectors <infile> <outfile>\n", progName);
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
                    System.out.printf("Usage: %s make-postings <index> <vocab-file> <offset> <max-documents-to-parse>\n", progName);
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
                    System.err.println("Deleting existing postings");
                    idx.deletePostings();
                    System.err.println("Creating new empty postings");
                    idx.addEmptyPostings(vocab);
                } else {
                    System.out.printf("Usage: %s reset-postings <index> <vocab-file>\n", progName);
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
                    System.err.printf("Usage: %s create-empty-index <index-location>\n", progName);
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
                    System.err.printf("Usage: %s add-documents <index-location> <vocab-file> <corpus-file> <corpus-offset> <max-to-add>\n", progName);
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
                    System.err.printf("Usage: %s calculate-vectors <index-location> <word-vector-file> <corpus-offset> <max-to-process>\n", progName);
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
                    System.err.printf("Usage: %s ccluster <index-location> <num-cluster-passes> <corpus-offset> <max-docs-to-cluster>\n", progName);
                }
                break;
            }
            case "cluster-cbor-query": {
            
                if (args.length == 5) {
                
                    final String dbname = args[1];
                    final String cborQueryFile = args[2];
                    final String facetMergeType = args[3];
                    final String runfile = args[4];
                    final int resultsPerQuery = 20;
                    // generate list of queries
                    // facetedQueries = Map<queryid, List<queryFacets>>
                    // queryFacet = List<queryTerms>
                    //
                    // Each query facet should be treated like a normal query.
                    // For each query id, we query each individual facet query, and merge the results.
                    Map<String, List<List<String>>> facetedQueries = Preprocess.preprocessFacetedQueries(cborQueryFile);

                    Index idx = Index.load(dbname).get();

                    //FileWriter outFile = new FileWriter(String.format("WordSimilarity+Clustering-Y3.run"));
                    FileWriter outFile = new FileWriter(runfile);

                    facetedQueries.forEach((queryID, facets) -> {
                        final List<List<Pair<String, Double>>> allFacetResults = new LinkedList<>();
                        facets.forEach((List<String> queryFacet) -> {
                            // List<DocID, Score>
                            final List<Pair<String, Double>> facetResults = Clustering.query(idx, queryFacet, resultsPerQuery);
                            allFacetResults.add(facetResults);
                        });
                        final List<Pair<String, Double>> finalResult = Merge_Queries.mergeFacets(allFacetResults, facetMergeType, 20);

                        AtomicInteger i = new AtomicInteger();
                        finalResult.forEach(p -> {
                            try {
                                outFile.write(String.format("%s Q0 %s %d %f TeamHotel-%s\n", queryID, p.getLeft(), i.get(), p.getRight(), "WordSimilarity"));
                            } catch (IOException ex) {
                                ex.printStackTrace();
                            }
                            idx.logResult(queryID, p.getLeft(), p.getRight(), i.getAndIncrement());
                        });

                    });
                } else {
                    System.out.printf("Usage: %s cluster-cbor-query index-name cborQueryFile\n", progName);
                }
                break;
            }
            case "tfidf-cbor-query": {
                if (args.length == 8) {
                    final String dbname = args[1];
                    final String cborQueryFile = args[2];
                    final String qrelFile = args[3];
                    final String tfidfVariant = args[4];
                    final String filterScored = args[5];
                    final String mergeType = args[6];
                    final String runfileName = args[7];

                    final Index idx = Index.load(dbname).get();
                    final Map<String, Set<String>> qrelDocs = Preprocess.getScoredQrelDocs(qrelFile).get();
                    final Map<String, List<List<String>>> facetedQueries = getFacetedQueries(cborQueryFile, qrelFile);
                    final String modelName = "tfidf_atc.btc";
                    FileWriter logfile = new FileWriter("queryLog.txt", true);
                    //FileWriter runFile = new FileWriter(String.format("tfidf-%s-TeamHotel.run", tfidfVariant.replace('.', '_')));
                    FileWriter runFile = new FileWriter(runfileName);

                    Map<String, List<List<Pair<String, Double>>>> queryResults = new TreeMap<>();
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
                            //System.out.println();
                            List<Pair<String, Double>> facetResults = Merge_Queries.queryTfidf(idx, terms, tfidfVariant, logfile, 1000);
                            queryResults.get(queryId).add(facetResults);
                            //System.out.printf("Facet has %d documents\n", facetResults.size());
                            //System.out.printf("Query %s has %d facet results\n", queryId, queryResults.get(queryId).size());
                        });
                    });

                    Map<String, List<Pair<String, Double>>> finalResults = new HashMap<>(queryResults.size() * 2);
                    queryResults.forEach((qid, facets) -> {
                        //System.err.printf("qid %s has %d facets\n", qid, facets.size());
                        List<Pair<String, Double>> result;
                        if (filterScored.toLowerCase().equals("filter")) {
                            result = Merge_Queries.filterUnscored(Merge_Queries.mergeFacets(facets, mergeType, 1000), qrelDocs.getOrDefault(qid, new HashSet<String>()), 20);
                        }
                        else {
                            result = Merge_Queries.mergeFacets(facets, mergeType, 20);
                        }
                        //System.err.printf("merging them together we get %d ranked documents\n", result.size());
                        finalResults.put(qid, result);
                    });

                    //System.err.printf("Final results contains results for %d queries\n", finalResults.size());

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
                    System.out.printf("Usage: %s tfidf-cbor-query <index-file> <cbor-query-file> <qrel> <qqq.ddd> <filter|nofilter>\n", progName);
                }
                break;
            }
            case "bm25-cbor-query": {
                if (args.length == 10) {
                    final String dbname = args[1];
                    final String cborQueryFile = args[2];
                    final String qrelFile = args[3];
                    //final String mergeType = args[3].toUpperCase();
                    //assert (mergeType.equals("AND") || mergeType.equals("OR"));
                    final double k1 = Double.parseDouble(args[4]);
                    final double k3 = Double.parseDouble(args[5]);
                    final double alpha = Double.parseDouble(args[6]);
                    final String filterScored = args[7];
                    final String mergeType = args[8];
                    final String runfileName = args[9];
                    // preprocess cbor queries.
                    // execute queries in sequence.
                    final Index idx = Index.load(dbname).get();

                    final Map<String, Set<String>> qrelDocs = Preprocess.getScoredQrelDocs(qrelFile).get();

                    final Map<String, List<List<String>>> facetedQueries = Preprocess.preprocessFacetedQueries(cborQueryFile);

                    // remove queries without qrel evaluation data
                    final List<String> toRemove = new LinkedList<>();
                    facetedQueries.keySet().forEach(qid -> {
                        if (!qrelDocs.keySet().contains(qid)) {
                            toRemove.add(qid);
                        }
                    });
                    toRemove.forEach(qid -> facetedQueries.remove(qid));

                    FileWriter logfile = new FileWriter("queryLog.txt");
                    //FileWriter runFile = new FileWriter(String.format("bm25_%.2f_%.2f_%.2f-TeamHotel.run", k1, k3, alpha));
                    FileWriter runFile = new FileWriter(runfileName);

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
                            //System.out.println();
                            List<Pair<String, Double>> facetResults = Merge_Queries.queryBM25(idx, terms, k1, k3, alpha, logfile, 1000);
                            queryResults.get(queryId).add(facetResults);
                            //System.out.printf("Facet has %d documents\n", facetResults.size());
                            //System.out.printf("Query %s has %d facet results\n", queryId, queryResults.get(queryId).size());
                        });
                    });

                    final String modelName = "bm25";
                    final String teamName = "TeamHotel";

                    Map<String, List<Pair<String, Double>>> finalResults = new HashMap<>(queryResults.size() * 2);
                    queryResults.forEach((qid, facets) -> {
                        //System.err.printf("qid %s has %d facets\n", qid, facets.size());
                        List<Pair<String, Double>> result;
                        if (filterScored.toLowerCase().equals("filter")) {
                            result = Merge_Queries.filterUnscored(Merge_Queries.mergeFacets(facets, mergeType, 1000), qrelDocs.getOrDefault(qid, new HashSet<String>()), 20);
                        }
                        else {
                            result = Merge_Queries.mergeFacets(facets, mergeType, 20);
                        }
                        //System.err.printf("merging them together we get %d ranked documents\n", result.size());
                        finalResults.put(qid, result);
                    });

                    //System.err.printf("Final results contains results for %d queries\n", finalResults.size());

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
                    System.out.printf("Usage: %s bm25-cbor-query <index> <cbor-query-file> <qrel> <filter|nofilter> <k1> <k3> <alpha>\n", progName);
                }
                break;
            }
            case "jelinekMercerCborQuery": {
                //Check is args.length equality is correct
                if (args.length == 8) {
                    final String dbname = args[1];
                    final String cborQueryFile = args[2];
                    final String qrelFile = args[3];
                    //final String mergeType = args[3].toUpperCase();
                    //assert (mergeType.equals("AND") || mergeType.equals("OR"));
                    final double beta = Double.parseDouble(args[4]);
                    final String filterScored = args[5];
                    final String mergeType = args[6];
                    final String runfileName = args[7];
                    // preprocess cbor queries.
                    // execute queries in sequence.
                    final Index idx = Index.load(dbname).get();

                    final Map<String, Set<String>> qrelDocs = Preprocess.getScoredQrelDocs(qrelFile).get();

                    final Map<String, List<List<String>>> facetedQueries = Preprocess.preprocessFacetedQueries(cborQueryFile);

                    // remove queries without qrel evaluation data
                    final List<String> toRemove = new LinkedList<>();
                    facetedQueries.keySet().forEach(qid -> {
                        if (!qrelDocs.keySet().contains(qid)) {
                            toRemove.add(qid);
                        }
                    });
                    toRemove.forEach(qid -> facetedQueries.remove(qid));

                    FileWriter logfile = new FileWriter("queryLog.txt");

                    //Check Proper Formating for runFile**************************
                    //FileWriter runFile = new FileWriter(String.format("jelinekMercer_%.2f-TeamHotel.run", beta));
                    FileWriter runFile = new FileWriter(runfileName);

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
                            //System.out.println();
                            List<Pair<String, Double>> facetResults = Merge_Queries.queryJelinekMercer(idx, terms, beta, logfile, 1000);
                            queryResults.get(queryId).add(facetResults);
                            //System.out.printf("Facet has %d documents\n", facetResults.size());
                            //System.out.printf("Query %s has %d facet results\n", queryId, queryResults.get(queryId).size());
                        });
                    });

                    final String modelName = "jelinekMercer";
                    final String teamName = "TeamHotel";

                    Map<String, List<Pair<String, Double>>> finalResults = new HashMap<>(queryResults.size() * 2);
                    queryResults.forEach((qid, facets) -> {
                        //System.err.printf("qid %s has %d facets\n", qid, facets.size());
                        List<Pair<String, Double>> result;
                        if (filterScored.toLowerCase().equals("filter")) {
                            result = Merge_Queries.filterUnscored(Merge_Queries.mergeFacets(facets, mergeType, 1000), qrelDocs.getOrDefault(qid, new HashSet<String>()), 20);
                        }
                        else {
                            result = Merge_Queries.mergeFacets(facets, mergeType, 20);
                        }
                        //System.err.printf("merging them together we get %d ranked documents\n", result.size());
                        finalResults.put(qid, result);
                    });

                    //System.err.printf("Final results contains results for %d queries\n", finalResults.size());

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
                    System.out.printf("Usage: %s bm25-cbor-query <index> <cbor-query-file> <qrel> <filter|nofilter> <k1> <k2> <k3> <alpha>\n", progName);
                }
                break;
            }
            case "query-ids": {
                if (args.length == 2) {
                    final String queryFile = args[1];
                    Preprocess.dumpQueryIds(queryFile);
                } else {
                    System.err.printf("Usage: %s query-ids <cborOutlines>\n", progName);
                }
                break;
            }
            case "mark-scored-documents": {
                if (args.length == 3) {
                    final String dbname = args[1];
                    final String docFile = args[2];
                    List<String> scoredDocs = Preprocess.loadVocab(docFile);
                    Index idx = Index.load(dbname).get();
                    idx.beginTransaction();
                    scoredDocs.forEach(id -> idx.markDocumentRelevant(id));
                    idx.commitTransaction();
                }
                break;
            }case "bim": {
                if (args.length == 7) {
                    final String dbname = args[1];
                    final String cborQueryFile = args[2];
                    final String qrelFile = args[3];
                    //final String mergeType = args[3].toUpperCase();
                    //assert (mergeType.equals("AND") || mergeType.equals("OR"));
                    final String filterScored = args[4];
                    final String mergeType = args[5];
                    final String runfileName = args[6];
                    //final String smoothing = args[5]; // l or j
                    final double a = 1;
			        final double b = 2;
  
                    
                    // preprocess cbor queries.
                    // execute queries in sequence.
                    final Index idx = Index.load(dbname).get();

                    final Map<String, Set<String>> qrelDocs = Preprocess.getScoredQrelDocs(qrelFile).get();

                    final Map<String, List<List<String>>> facetedQueries = Preprocess.preprocessFacetedQueries(cborQueryFile);

                    // remove queries without qrel evaluation data
                    final List<String> toRemove = new LinkedList<>();
                    facetedQueries.keySet().forEach(qid -> {
                        if (!qrelDocs.keySet().contains(qid)) {
                            toRemove.add(qid);
                        }
                    });
                    toRemove.forEach(qid -> facetedQueries.remove(qid));

                    FileWriter logfile = new FileWriter("queryLog.txt");
                    //FileWriter runFile = new FileWriter(String.format("bim-TeamHotel.run"));
                    FileWriter runFile = new FileWriter(runfileName);

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
                            //System.out.println();
                            List<Pair<String, Double>> facetResults = Merge_Queries.queryBIM(idx, terms, a, b, logfile, 1000);
                            queryResults.get(queryId).add(facetResults);
                            //System.out.printf("Facet has %d documents\n", facetResults.size());
                            //System.out.printf("Query %s has %d facet results\n", queryId, queryResults.get(queryId).size());
                        });
                    });

                    final String modelName = "bim";
                    final String teamName = "TeamHotel";

                    Map<String, List<Pair<String, Double>>> finalResults = new HashMap<>(queryResults.size() * 2);
                    queryResults.forEach((qid, facets) -> {
                        //System.err.printf("qid %s has %d facets\n", qid, facets.size());
                        List<Pair<String, Double>> result;
                        if (filterScored.toLowerCase().equals("filter")) {
                            result = Merge_Queries.filterUnscored(Merge_Queries.mergeFacets(facets, mergeType, 1000), qrelDocs.getOrDefault(qid, new HashSet<String>()), 20);
                        }
                        else {
                            result = Merge_Queries.mergeFacets(facets, mergeType, 20);
                        }
                        //System.err.printf("merging them together we get %d ranked documents\n", result.size());
                        finalResults.put(qid, result);
                    });

                    //System.err.printf("Final results contains results for %d queries\n", finalResults.size());

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
                    System.out.printf("Usage: %s bim <index> <cbor-query-file> <qrel> <filter|nofilter>\n", progName);
                }
                break;

            } case "list-queries": {
                if (args.length == 3) {
                    final String cborQueryFile = args[1];
                    final String qrelFile = args[2];
                    final Map<String, Set<String>> qrelDocs = Preprocess.getScoredQrelDocs(qrelFile).get();

                    final Map<String, List<List<String>>> facetedQueries = Preprocess.preprocessFacetedQueries(cborQueryFile);

                    // remove queries without qrel evaluation data
                    final List<String> toRemove = new LinkedList<>();
                    facetedQueries.keySet().forEach(qid -> {
                        if (!qrelDocs.keySet().contains(qid)) {
                            toRemove.add(qid);
                        }
                    });
                    toRemove.forEach(qid -> facetedQueries.remove(qid));

                    facetedQueries.forEach((qid, facets) -> {
                        System.out.printf("Query ID: %s\n", qid);
                        facets.forEach(terms -> {
                            System.out.print("Facet:");
                            terms.forEach(term -> System.out.printf( "%s", term));
                            System.out.println();
                        });
                    });
                } else {
                    System.out.printf("Usage: %s list-queries <cborOutlines> <qrel>");
                }
                break;
            }

            default:
            System.out.printf("Usage: %s [query-vocab | corpus-vocab | preprocess-similarity-vectors\n | create-empty-index | add-documents | calculate-vectors\n | cluster | cluster-cbor-query | make-postings | reset-postings]\n" +
            "Run commands for specific usage instructions\n", progName);
        }
    }

    static Map<String, List<List<String>>> getFacetedQueries(final String cborOutlines, final String qrel) {
        final Map<String, List<List<String>>> facetedQueries = Preprocess.preprocessFacetedQueries(cborOutlines);
        final Map<String, Set<String>> qrelDocs = Preprocess.getScoredQrelDocs(qrel).get();

        // remove queries without qrel evaluation data
        final List<String> toRemove = new LinkedList<>();
        facetedQueries.keySet().forEach(qid -> {
            if (!qrelDocs.keySet().contains(qid)) {
                toRemove.add(qid);
            }
        });
        toRemove.forEach(qid -> facetedQueries.remove(qid));

        return facetedQueries;
    }
}
