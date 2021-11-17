package com.TeamHotel.main;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

import com.TeamHotel.inverindex.Index;
import com.TeamHotel.preprocessor.Preprocess;

import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

public class WordSimilarity {
    public final static int numDimensions = 100; //? change this to actual number of dimensions in word vectors

    public static int calculateDocVectors(Index idx, String wordVectorFile, int offset, int maxDocuments) {
        // load word vectors.
        // Preprocess.preprocessWord(words) will stem each of the words.  It can return an empty string, in which case
        // we should ignore the word.
        HashMap<String, ArrayList<Double>> wordVectors = new HashMap<String, ArrayList<Double>>();
        wordVectorfile(wordVectorFile,wordVectors);
        Iterator<Pair<String, Map<String, Integer>>> documentIterator = idx.getTokenizedDocuments(offset, maxDocuments);

        idx.beginTransaction(); // this speeds up database operations
        
        documentIterator.forEachRemaining(pair -> {
            final String docID = pair.getLeft();
            final Map<String, Integer> termFrequencies = pair.getRight();
            System.out.printf("Calculating vector for document %s\n", docID);
            ArrayList<Double> docVector = new ArrayList<>(Collections.nCopies(numDimensions, 0.0));
            AtomicInteger counter = new AtomicInteger();
            ArrayList<Double> wordVector = new ArrayList<>(Collections.nCopies(numDimensions, 0.0));

            for (Map.Entry<String, Integer> e: termFrequencies.entrySet()) {
                String term = e.getKey();
                // get word vector from file
                ArrayList<Double> wordVec = wordVectors.get(term);
                if (wordVec != null) {
                    addVectors(wordVector,wordVectors.get(term));
                }
                //System.out.println(wordVectors.get(term));
                counter.incrementAndGet();

            }
            int num = counter.intValue();
            averVectors(docVector,wordVector,num);
            // doc vector = average word 
            // print vector perhaps
            //System.out.println(docVector.size());
            idx.setDocumentVector(docID, docVector);
        });

        idx.commitTransaction(); // if we run out of memory, removing this and beginTransaction() should help.

        return 0;
    }

    public static int preprocessWordVectors(@NotNull final Scanner inScanner, @NotNull final FileWriter outfile) throws IOException {
        int numProcessed = 0;
        while (inScanner.hasNext()) {
            final String word = Preprocess.preprocessWord(inScanner.next());
            if (!word.isEmpty()) {
                outfile.write(word);
                for (int i = 0; i < numDimensions; i++) {
                    final String d = inScanner.next();
                    Double.parseDouble(d);
                    outfile.write(String.format(" %s", d));
                }
                outfile.write("\n");
                numProcessed++;
                if (numProcessed % 20000 == 0) {
                    System.err.printf("Processed %d word vectors\n", numProcessed);
                }
            } else {
                for (int i = 0; i < numDimensions; i++) {
                    inScanner.next();
                }
            }
        }
        return numProcessed;
    }

    public static int wordVectorfile(String filename,HashMap<String, ArrayList<Double>> map) {
        System.out.println("Loading word vectors");
        try {
            File myObj = new File(filename);
            Scanner myReader = new Scanner(myObj);
            int numLoaded = 0;
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                String[] parts = data.split(" ");
                ArrayList<Double> vector = new ArrayList<>();
                String word = parts[0];
                //System.out.println(word);
        
                for(int i = 1;i < parts.length; i++)
                {
                    vector.add(Double.parseDouble(parts[i]));
                }
                map.put(word, vector);
                numLoaded++;
                if (numLoaded % 20000 == 0) {
                    System.out.printf("Loaded %d word vectors\n", numLoaded);
                }
            }
            myReader.close();
            } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();

        }
        System.out.println("Finished loading word vectors");
        return 0;
    }

    public static boolean isNumeric(String strNum) {
        if (strNum == null) {
            return false;
        }
        try {
            Double.parseDouble(strNum);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

    public static int addVectors(ArrayList<Double> vector1, ArrayList<Double> vector2) {
        for (int i = 0; i < vector1.size();i++){
            vector1.set(i, vector2.get(i) + vector1.get(i));
            //System.out.println(vector1.get(i));
        }
        //System.out.println(vector1);
    
        return 0;
    }

    public static int averVectors(ArrayList<Double> doc, ArrayList<Double> vector, int count) {
        System.out.println(count);
        for (int i = 0; i < doc.size();i++){
            doc.set(i, vector.get(i) / count);
        }
        return 0;
    }
}
