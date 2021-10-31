package com.TeamHotel.preprocessor;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import java.io.FileInputStream;
import java.io.InputStream;

import edu.unh.cs.treccar_v2.Data;
import edu.unh.cs.treccar_v2.read_data.DeserializeData;

import org.jetbrains.annotations.NotNull;
import org.tartarus.snowball.SnowballStemmer;
import org.tartarus.snowball.ext.porterStemmer;


public class Preprocess {
    final static private int _minTokenSize = 3;
    final static private String _stopWordsFile = "stop.txt";
    static private Set<String> stopWords = null;
    static private boolean stopWordsLoaded = false;

    public enum QueryType { SECTIONS, PAGES }

    public static List<String> loadVocab(@NotNull final String vocabFile) {
        try {
            Scanner sc = new Scanner(new FileInputStream(vocabFile));
            List<String> vocab = new LinkedList<>();
            sc.forEachRemaining(vocab::add);
            return vocab;
        } catch (Exception ex) {
            ex.printStackTrace();
            return new LinkedList<>();
        }
    }

    public static ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> preprocessCborQueries(final String cborOutlineFile, final QueryType type) {
        ConcurrentHashMap<String, String> queries = new ConcurrentHashMap<>();
        try {

            final FileInputStream queryStream  = new FileInputStream(cborOutlineFile);

            if (type == QueryType.SECTIONS) {
                for (final Data.Page query : DeserializeData.iterableAnnotations(queryStream)) {
                    for (List<Data.Section> sections : query.flatSectionPaths()) {
                        final String queryId = Data.sectionPathId(query.getPageId(), sections);
                        final StringBuilder queryBuilder = new StringBuilder(query.getPageName());
                        sections.forEach(section -> queryBuilder.append(" ").append(section.getHeading()));
                        final String content = queryBuilder.toString();
                        queries.put(queryId, content);
                    }
                }
            } else if (type == QueryType.PAGES) {
                for (final Data.Page query : DeserializeData.iterableAnnotations(queryStream)) {
                    final String queryId = query.getPageId();
                    final String content = query.getPageName();
                    queries.put(queryId, content);
                }
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

    static AtomicInteger ids = new AtomicInteger(0);
    static AtomicInteger numProcessed = new AtomicInteger(0);
    
    public static ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> preprocessLargeCborParagrphsWithVocab(final String cborFile, Set<String> vocab, int offset, int maxDocuments) {
        ids = new AtomicInteger(0);
        numProcessed = new AtomicInteger(0);
        int numThreads = 6;
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
        
        for (int i = 0; i < offset; i++) documentIterator.next(); // This will throw if the offset is greater than the total number of paragraphs.
        
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
                        .filter(w -> vocab == null || vocab.contains(w))
                        // associate terms with term-frequency in map
                        .forEach(term -> {
                            Integer occurrances = termFrequencies.putIfAbsent(term, 1);
                            if (occurrances != null) {
                                termFrequencies.put(term, occurrances + 1);
                            }
                        });
                        final String docId = paragraph.getParaId();
                        if (!termFrequencies.isEmpty()) {
                            documents.put(docId, new ConcurrentHashMap<>(termFrequencies));
                        }
                        progress[tid]++;
                        int totalDone = numProcessed.incrementAndGet();
                        if (totalDone % 20000 == 0) {
                            System.out.printf("Processed %d documents\n", totalDone);
                        }
                        if (totalDone % 100000 == 0) {
                            System.gc();
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

    public static void processParagraphs(String cborParagraphs, Object object) {
    }

    public interface AddDocumentsInterface {
        void newDoc(String id, String text, List<String> tokenized, TreeMap<String, Integer> tokens);
    }

    public static int processParagraphs(@NotNull final String cborFile, @NotNull final AddDocumentsInterface add, Set<String> vocab, int offset, int maxDocuments) {
        ids = new AtomicInteger(0);
        numProcessed = new AtomicInteger(0);
        int numThreads = 1;
        System.err.println("Preprocessing a large cbor file");
        final Set<String> stopWords = loadStopWords();
        System.err.println("Stopwords loaded");
        Thread[] threads = new Thread[numThreads];
        int[] progress = new int[numThreads];

        Iterator<Data.Paragraph> documentIterator;
        try {
            final FileInputStream documentStream  = new FileInputStream(cborFile);
            System.err.println("file opened");
            documentIterator = DeserializeData.iterParagraphs(documentStream);
        } catch (Exception ex) {
            ex.printStackTrace();
            return 0;
        }
        
        for (int i = 0; i < offset; i++) documentIterator.next(); // This will throw if the offset is greater than the total number of paragraphs.
        
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
                        final String fulltext = paragraph.getTextOnly();

                        // paragraph text -> token stream
                        final List<String> tokenizedFulltext = List.of(fulltext.toLowerCase().replaceAll("\\p{Punct}|\\d|\\p{Cntrl}", " ").split("\\s+")).stream()
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
                        .filter(w -> vocab == null || vocab.contains(w)).collect(Collectors.toList());

                        // associate terms with term-frequency in map
                        final TreeMap<String, Integer> termFrequencies = new TreeMap<>();
                        tokenizedFulltext.forEach(term -> {
                            Integer occurrances = termFrequencies.putIfAbsent(term, 1);
                            if (occurrances != null) {
                                termFrequencies.put(term, occurrances + 1);
                            }
                        });

                        final String docId = paragraph.getParaId();
                        if (!termFrequencies.isEmpty()) {
                            add.newDoc(docId, fulltext, tokenizedFulltext, termFrequencies);
                        }
                        progress[tid]++;
                        int totalDone = numProcessed.incrementAndGet();
                        if (totalDone % 20000 == 0) {
                            System.out.printf("Processed %d documents\n", totalDone);
                        }
                        if (totalDone % 100000 == 0) {
                            System.gc();
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

        for (int i = 0; i < numThreads; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return numProcessed.getPlain();
    }


    public static Map<String, List<List<String>>> readFacetedQueries(String cborQueryFile) {
        return null;
    }
}
