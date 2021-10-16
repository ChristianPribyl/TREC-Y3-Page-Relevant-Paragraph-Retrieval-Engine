package com.TeamHotel.preprocessor;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Triple;
import java.io.FileInputStream;
import java.io.InputStream;

import edu.unh.cs.treccar_v2.Data;
import edu.unh.cs.treccar_v2.Data.Paragraph;
import edu.unh.cs.treccar_v2.read_data.DeserializeData;
import org.tartarus.snowball.SnowballStemmer;
import org.tartarus.snowball.ext.porterStemmer;


public class Preprocess {
    final static private int _minTokenSize = 3;
    final static private String _stopWordsFile = "stop.txt";
    static private Set<String> stopWords = null;
    static private boolean stopWordsLoaded = false;

    public static Map<Integer, Triple<String, String, Map<String, Integer>>> preprocessCborKeepEverything(final String cborParagraphsFile) {
        System.err.println("Preprocessing with fulltext and custom ids");
        Map<String, String> rawText = loadTest200Text(cborParagraphsFile);
        ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> processedText = preprocess(rawText);
        Map<Integer, Triple<String, String, Map<String, Integer>>> result = new HashMap<>();
        int i = 0;
        for (String id: rawText.keySet()) {
            result.put(i, Triple.of(id, rawText.get(id), processedText.get(id)));  
            i++;          
        }
        System.err.println("Finished preprocessing");
        return result;
    }

    public static ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> preprocessCborParagraphs(final String cborParagraphsFile) {
        return preprocess(loadTest200Text(cborParagraphsFile));
    }

    public static String preprocessWord(final String word) {
        Map<String, String> query = new HashMap<>();
        query.put("query", word);
        Optional<ConcurrentHashMap<String, Integer>> value = preprocess(query).values().stream().findAny();
        if (value.isPresent()) {
            Optional<String> term = value.get().keySet().stream().findAny();
            if (term.isPresent()) {
                return term.get();
            }
        }
        return "";
    }

    public static ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> preprocessCborQueries(final String cborOutlineFile) {
        ConcurrentHashMap<String, String> queries = new ConcurrentHashMap<>();
        try {

            final FileInputStream queryStream  = new FileInputStream(cborOutlineFile);

            for (final Data.Page query : DeserializeData.iterableAnnotations(queryStream)) {
                final String content = query.getPageName();
                final String queryId = query.getPageId();

                queries.put(queryId, content);
            }
        }
        catch(Exception ex) {   
            ex.printStackTrace(System.err);
        }

        return preprocess(queries);
    }

    public static Set<String> getVocabulary(final ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> paragraphs) {
        return paragraphs.values().stream().flatMap(m -> m.keySet().stream()).collect(Collectors.toSet());
    }

    public static void printVocabulary(Set<String> words) {
        System.err.printf("Printing vocabulary with %d terms\n", words.size());

        Set<String> sortedWords = new TreeSet<>(words);
        sortedWords.forEach(word -> {
            if (word.contains("?")) {
                System.err.printf("%s contains ?\n", word);
            }
            System.out.println(word);
        });
    }

    public static ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> preprocess(final Map<String, String> paragraphs) {
        if (!stopWordsLoaded) {
            stopWords = loadStopWords();
            stopWordsLoaded = true;
        }
        return dropTermsSmallerThan(stemTokens(removeStopwords(tokenizeParagraphs(paragraphs), stopWords)));
    }


// private methods

    private static Set<String> loadStopWords() {
        Set<String> stopWords = new HashSet<>();
        try {
            InputStream file = Thread.currentThread().getContextClassLoader().getResourceAsStream(Preprocess._stopWordsFile);
            assert file != null;
            final Scanner sc = new Scanner(file);
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                if (line.indexOf('|') != -1) {
                    line = line.substring(0, line.indexOf('|'));
                }
                final String word = line.trim();
                if (!word.isEmpty()) {
                    stopWords.add(word);
                }
            }
            sc.close();
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
            return new HashSet<>();
        }
        return stopWords;
    }

    /**
     * loadTest200Text loads the content of a test200 cbor paragraphs file into a map.
     * @param cborParagraphsFile - The file containing cbor paragraph data.
     * @return Map<String, String> - A map with keys = paragraph ids and content = paragraph text
     *         If any error occurs internally, an empty map is returned.
     */
    private static Map<String, String> loadTest200Text(final String cborParagraphsFile) {
        final Map<String, String> paragraphs = new HashMap<>();
        try {
            final FileInputStream paragraphStream  = new FileInputStream(cborParagraphsFile);
                
            for (final Iterator<Data.Paragraph> paragraphIterator = DeserializeData.iterParagraphs(paragraphStream); paragraphIterator.hasNext();) {
                final Paragraph paragraph = paragraphIterator.next();
                final String content = paragraph.getTextOnly();
                final String paraId = paragraph.getParaId();

                paragraphs.put(paraId, content);
            }

        } catch (Exception ex) {
            ex.printStackTrace(System.err);
            return new HashMap<>();
        }

        return paragraphs;
    }

    final static AtomicInteger ids = new AtomicInteger(0);
    final static AtomicInteger numProcessed = new AtomicInteger(0);

    public static ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> preprocessLargeCborParagrphs(final String cborFile) {
        int numThreads = 6;
        int maxDocuments = 1000000;
        System.err.println("Preprocessing a large cbor file");
        final Set<String> stopWords = loadStopWords();
        System.err.println("Stopwords loaded");
        final ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> documents = new ConcurrentHashMap<>(40000000);
        Thread[] threads = new Thread[numThreads];
        int[] progress = new int[numThreads];

        Iterator<Data.Paragraph> documentIterator;
        try {
            final FileInputStream documentStream  = new FileInputStream(cborFile);
            System.err.println("file opened");
            documentIterator = DeserializeData.iterParagraphs(documentStream);
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
        
        for (int i = 0; i < numThreads; i++) {
            threads[i] = (new Thread(new Runnable(){
                public void run() {
                    int tid = ids.incrementAndGet() - 1;
                    SnowballStemmer stemmer = new porterStemmer();
                    while (true) {
                        Data.Paragraph paragraph;
                        synchronized(documentIterator) {
                            if (documentIterator.hasNext())
                                paragraph = documentIterator.next();
                            else
                                break;
                        }
                        final String content = paragraph.getTextOnly();
                        final Map<String, Integer> termFrequencies = new TreeMap<>();

                        // paragraph text -> token stream
                        List.of(content.toLowerCase().replaceAll("\\p{Punct}|\\d|\\p{Cntrl}", " ").split("\\s+")).stream()
                        // remove stopwords
                        .filter(w -> !stopWords.contains(w))
                        // stem tokens
                        .map(w -> {
                            stemmer.setCurrent(w);
                            stemmer.stem();
                            return stemmer.getCurrent();
                        })
                        // drop small terms
                        .filter(t -> (t.length() > _minTokenSize))
                        // associate terms with term-frequency in map
                        .forEach(term -> {
                            Integer occurrances = termFrequencies.putIfAbsent(term, 1);
                            if (occurrances != null) {
                                termFrequencies.put(term, occurrances + 1);
                            }
                        });
                        final String docId = paragraph.getParaId();
                        documents.put(docId, new ConcurrentHashMap<>(termFrequencies));
                        progress[tid]++;
                        int totalDone = numProcessed.incrementAndGet();
                        if (totalDone % 10000 == 0) {
                            System.out.printf("Processed %d documents\n", totalDone);
                        }
                        if (totalDone >= maxDocuments) {
                            break;
                        }
                    }
                    System.err.printf("Thread %d done!  It processed %d documents\n", tid, progress[tid]);
                }
            }));
            threads[i].start();
        }
              
        System.err.println("Waiting for threads to finish");

        for (Thread t: threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
                return null;
            }
        }

        System.err.println("Preprocessed cbor file");
        return documents;

    }

    
    public static ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> preprocessLargeCborParagrphsWithVocab(final String cborFile, Set<String> vocab) {
        int numThreads = 6;
        int maxDocuments = 1000000;
        System.err.println("Preprocessing a large cbor file");
        final Set<String> stopWords = loadStopWords();
        System.err.println("Stopwords loaded");
        final ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> documents = new ConcurrentHashMap<>(40000000);
        Thread[] threads = new Thread[numThreads];
        int[] progress = new int[numThreads];

        Iterator<Data.Paragraph> documentIterator;
        try {
            final FileInputStream documentStream  = new FileInputStream(cborFile);
            System.err.println("file opened");
            documentIterator = DeserializeData.iterParagraphs(documentStream);
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
        
        for (int i = 0; i < numThreads; i++) {
            threads[i] = (new Thread(new Runnable(){
                public void run() {
                    int tid = ids.incrementAndGet() - 1;
                    SnowballStemmer stemmer = new porterStemmer();
                    while (true) {
                        Data.Paragraph paragraph;
                        synchronized(documentIterator) {
                            if (documentIterator.hasNext())
                                paragraph = documentIterator.next();
                            else
                                break;
                        }
                        final String content = paragraph.getTextOnly();
                        final Map<String, Integer> termFrequencies = new TreeMap<>();

                        // paragraph text -> token stream
                        List.of(content.toLowerCase().replaceAll("\\p{Punct}|\\d|\\p{Cntrl}", " ").split("\\s+")).stream()
                        // remove stopwords
                        .filter(w -> !stopWords.contains(w))
                        // stem tokens
                        .map(w -> {
                            stemmer.setCurrent(w);
                            stemmer.stem();
                            return stemmer.getCurrent();
                        })
                        // drop small terms
                        .filter(t -> (t.length() > _minTokenSize))
                        // drop terms not used for queries
                        .filter(w -> vocab.contains(w))
                        // associate terms with term-frequency in map
                        .forEach(term -> {
                            Integer occurrances = termFrequencies.putIfAbsent(term, 1);
                            if (occurrances != null) {
                                termFrequencies.put(term, occurrances + 1);
                            }
                        });
                        final String docId = paragraph.getParaId();
                        documents.put(docId, new ConcurrentHashMap<>(termFrequencies));
                        progress[tid]++;
                        int totalDone = numProcessed.incrementAndGet();
                        if (totalDone % 10000 == 0) {
                            System.out.printf("Processed %d documents\n", totalDone);
                        }
                        if (totalDone >= maxDocuments) {
                            break;
                        }
                    }
                    System.err.printf("Thread %d done!  It processed %d documents\n", tid, progress[tid]);
                }
            }));
            threads[i].start();
        }
              
        System.err.println("Waiting for threads to finish");

        for (Thread t: threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
                return null;
            }
        }

        System.err.println("Preprocessed cbor file");
        return documents;

    }

    private static Map<String, List<String>> tokenizeParagraphs(final Map<String, String> paragraphs) {
        Map<String, List<String>> tokenizedParagraphs = new HashMap<>();
        paragraphs.forEach((id, content) ->
                tokenizedParagraphs.put(id,
                // remove punctuation, unprintable chars, and digits
                List.of(content.toLowerCase().replaceAll("\\p{Punct}|\\d|\\p{Cntrl}", " ").split("\\s+"))));
        return tokenizedParagraphs;
    }

    private static Map<String, List<String>> removeStopwords(final Map<String, List<String>> tokenizedParagraphs, final Set<String> stopWords) {
        Map<String, List<String>> filteredMap = new HashMap<>();
        tokenizedParagraphs.forEach((id, tokens) ->
        {
            final List<String> filteredTokens = tokens.stream().filter(token -> !stopWords.contains(token)).collect(Collectors.toList());
            filteredMap.put(id, filteredTokens);
        });

        return filteredMap;
    }

    private static Map<String, Map<String, Integer>> stemTokens(final Map<String, List<String>> tokenizedParagraphs) {
        Map<String, Map<String, Integer>> stemmedParagraphs = new HashMap<>();
        SnowballStemmer stemmer = new porterStemmer();
        tokenizedParagraphs.forEach((id, tokens) ->
        {
            Map<String, Integer> stemmedTokens = new HashMap<>();
            tokens.forEach(token -> {
                stemmer.setCurrent(token);
                stemmer.stem();
                if (stemmedTokens.containsKey(token)) {
                    stemmedTokens.put(stemmer.getCurrent(), stemmedTokens.get(token) + 1);
                } else {
                    stemmedTokens.put(stemmer.getCurrent(), 1);
                }
            });
            stemmedParagraphs.put(id, stemmedTokens);
        });
        return stemmedParagraphs;
    }

    private static ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> dropTermsSmallerThan(final Map<String, Map<String, Integer>> tokenizedParagraphs) {
        Map<String, ConcurrentHashMap<String, Integer>> filteredMap = new HashMap<>();
        tokenizedParagraphs.forEach((id, tokenMap) -> {
            Map<String, Integer> filteredTokens = new HashMap<>();
            tokenMap.forEach((t, num) -> {
                if (t.length() >= Preprocess._minTokenSize) filteredTokens.put(t, num);
            });
            filteredMap.put(id, new ConcurrentHashMap<>(filteredTokens));
        });
        return new ConcurrentHashMap<>(filteredMap);
    }

}
