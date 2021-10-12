package com.TeamHotel.inverindex;

import com.TeamHotel.preprocessor.Preprocess;

import java.io.File;
import java.util.*;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.io.FileInputStream;
import org.apache.commons.lang3.tuple.Triple;


public class Index implements Serializable{
    public final String carId;
    public final Integer ourId;
    public final Map<String, Integer> termsSet;
    public final String fullText;
    public final IndexDocument indexDocument;

    public Index(String carId, Integer ourId, Map<String, Integer> termsSet, String fulltext)
    {
        this.carId = carId;
        this.ourId = ourId;
        this.termsSet = termsSet;
        this.fullText = fulltext;
        this.indexDocument = new IndexDocument(carId);
        indexDocument.setFulltext(fulltext);
        indexDocument.setQuality(1.0);
    }

    public Map<String, Integer> getTerms(){
        return termsSet;
    }
    public Integer getId(){
        return ourId;
    }

    public String getCarId() {
        return carId;
    }

    public String getFulltext() {
        return fullText;
    }
    
    public IndexDocument getDocument() {
        return this.indexDocument;
    }

    public static ArrayList<Index> createIndex(final String cborParagraphs)
    {
        Map<Integer, Triple<String, String, Map<String, Integer>>> document = Preprocess.preprocessCborKeepEverything(cborParagraphs);
        ArrayList<Index> list = new ArrayList<Index>();
        document.forEach((id, t) -> {
            Index idx = new Index(t.getLeft(), id, t.getRight(), t.getMiddle());
            list.add(idx);
        });

        return list;
    }

    public static boolean printIndex(final String indexFile)
    {
        try {
            FileInputStream documentIStream = new FileInputStream(indexFile);
            ObjectInputStream ois =new ObjectInputStream(documentIStream);
            Object obj = ois.readObject();
            ArrayList<Index> list = null;
            if (obj instanceof ArrayList) {
                ArrayList<Object>objList = (ArrayList<Object>)obj;
                if (objList.size() == 0) {
                    System.err.println("Empty index");
                }
                if (objList.get(0) instanceof Index) {
                    list = (ArrayList<Index>)obj;
                }
            }

            assert list != null;
            list.forEach((idx) -> {
                final Integer id = idx.getId();
                final Map<String, Integer> paragraph = idx.getTerms();
                System.out.printf("id: %s\nterms: ", id);
                paragraph.forEach((word, num) -> System.out.printf("%s ", word));
                System.out.println();
            });

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }

        return true;
    }
    
}
