package com.TeamHotel.main;

import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;

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


        Iterator<Pair<String, Map<String, Integer>>> documentIterator = idx.getTokenizedDocuments(offset, maxDocuments);

        idx.beginTransaction(); // this speeds up database operations

        documentIterator.forEachRemaining(pair -> {
            final String docID = pair.getLeft();
            final Map<String, Integer> termFrequencies = pair.getRight();
            
            ArrayList<Double> docVector = new ArrayList<>(numDimensions);
            termFrequencies.forEach((String term, Integer tf) -> {
                ArrayList<Double> wordVector = new ArrayList<>(numDimensions);
                // get word vector from file

            });

            // doc vector = average word 

            // print vector perhaps

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
                outfile.write(" ");
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
}
