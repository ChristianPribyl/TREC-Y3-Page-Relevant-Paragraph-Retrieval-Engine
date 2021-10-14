package com.TeamHotel.inverindex;

import com.TeamHotel.preprocessor.Preprocess;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.io.FileInputStream;


public class Index implements Serializable{
    public final String carId;
    public final ConcurrentHashMap<String, Integer> termsSet;
    public final IndexDocument indexDocument;

    public Index(String carId, ConcurrentHashMap<String, Integer> termsSet)
    {
        this.carId = carId;
        this.termsSet = termsSet;
        this.indexDocument = new IndexDocument(carId);
        indexDocument.setQuality(1.0);
    }

    public ConcurrentHashMap<String, Integer> getTerms(){
        return termsSet;
    }

    public String getCarId() {
        return carId;
    }
    
    public IndexDocument getDocument() {
        return this.indexDocument;
    }

    public static ArrayList<Index> createIndex(final String cborParagraphs)
    {
        Map<String, ConcurrentHashMap<String, Integer>>  document = Preprocess.preprocessLargeCborParagrphs(cborParagraphs);
        ArrayList<Index> list = new ArrayList<Index>();
        document.forEach((id, t) -> {
            Index idx = new Index(id, t);
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


        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }

        return true;
    }
    
}
