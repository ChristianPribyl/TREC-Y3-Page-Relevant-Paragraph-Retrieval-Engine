package com.TeamHotel.main;

import java.util.Iterator;
import java.util.Map;

import com.TeamHotel.inverindex.Index;

import org.apache.commons.lang3.tuple.Pair;

public class WordSimilarity {
    final private static int numDimensions = 100; //? change this to actual number

    public static void calculateWordVectors(Index idx, int offset) {
        Iterator<Pair<String, Map<String, Integer>>> documentIterator = idx.getTokenizedDocuments(offset, 100000000); // perhaps use smaller number for testing
        idx.beginTransaction(); // this speeds things up.

        documentIterator.forEachRemaining(pair -> {
            final String docID = pair.getLeft();
            final Map<String, Integer> termFrequencies = pair.getRight();
            
            double[] docVector = new double[numDimensions];
            termFrequencies.forEach((String term, Integer tf) -> {
                double[] wordVector = new double[numDimensions];
                // get word vector from file

            });

            // doc vector = average word 

            // print vector perhaps

            idx.setDocumentVector(docID, docVector);
        });

        idx.commitTransaction(); // if we run out of memory, removing this and beginTransaction() should help.
    }

}
