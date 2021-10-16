package com.TeamHotel.main;

import com.TeamHotel.preprocessor.Preprocess;
import com.TeamHotel.inverindex.*;

import com.TeamHotel.merge_queries.Merge_Queries;
import com.sun.source.tree.WhileLoopTree;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.io.FileWriter;
import java.io.IOException;

public class Main {
    public static void main(String[] args) {
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
                    final ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> queries = Preprocess.preprocessCborQueries(cborQueryFile);
                    vocabulary = Preprocess.getVocabulary(queries);
                } else if (args.length == 2) {
                    // args[1] cbor-paragraphs file

                    final String cborParagraphsFile = args[1];
                    // Map<paragraphsId, Map<term, tf>>
                    final ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> paragraphs = Preprocess.preprocessLargeCborParagrphs(cborParagraphsFile);
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
                    final InvertedIndex invertedIndex = InvertedIndex.createInvertedIndex(args[1], args[2]);
                    InvertedIndex.saveIndex(invertedIndex, args[3]);
                } else {
                    indexUsage();
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
                    final ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> queries = Preprocess.preprocessCborQueries(cborQueryFile);
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
            default:
                usage();
        }
    }

    static void usage() {
        System.out.println("Usage: prog-4 [vocab | index | dump-index | query | cbor-query]\n" +
                "Run commands for specific usage instructions");
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
