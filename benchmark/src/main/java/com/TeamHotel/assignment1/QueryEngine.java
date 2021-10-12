package com.TeamHotel.assignment1;

import java.util.*;
import java.lang.System;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import edu.unh.cs.treccar_v2.Data;
import edu.unh.cs.treccar_v2.read_data.DeserializeData;
public class QueryEngine {

   
    public static int rank( final String documentCBOR, final String indexPath){
        System.setProperty("file.encoding", "UTF-8");

        try {
            final FileInputStream file  = new FileInputStream(new File(documentCBOR));

            //search engine
            Directory dir = FSDirectory.open(Paths.get(indexPath));
            IndexReader reader = DirectoryReader.open(dir);
            IndexSearcher searcher = new IndexSearcher(reader);
            //5.1  Use the trec-car-tools to extract page ID and page name
            //6.1 Read queries from the test200 collection
            // file /test200/test200-train/train.pages.cbor-outlines.cbor.
            //adapted from: https://github.com/TREMA-UNH/trec-car-tools/blob/master/trec-car-tools-example/src/main/java/edu/unh/cs/TrecCarToolsExample.java
            for(Data.Page page: DeserializeData.iterableAnnotations(file)) {
               
                
        
            //5.2 Use the page name as a keyword query, and use the page ID as a queryId
            //    passing page name as query
                QueryParser parser = new QueryParser("text",new StandardAnalyzer());
                Query query = parser.parse(page.getPageName());
            //6.2 query retrieve the top 100 documents
                searcher.setSimilarity(new BM25Similarity());
                TopDocs hits = searcher.search(query,100);
                System.out.println("\nquery: "+ query + "");
                System.out.println("Total Results :: " + hits.totalHits);
            //6.3 display results and -> to .run file
            //3.1 For each keyword query, you produce a ranking
            //adapted from: https://github.com/TREMA-UNH/trec-car-tools/blob/master/trec-car-tools-example/src/main/java/edu/unh/cs/TrecCarQueryLuceneIndex.java
                ScoreDoc[] scoreDocs = hits.scoreDocs;
                    for (int i = 0; i < scoreDocs.length; i++) {
                        ScoreDoc score = scoreDocs[i];
                        final Document doc = searcher.doc(score.doc); 
                        final String paragraphid = doc.getField("paragraphid").stringValue();
                        final float searchScore = score.score;
                        final int searchRank = i+1;
            //3.2Write all rankings to the same file using the following format (rankings concatenated, one line per ranked item):
                        System.out.println(page.getPageId()+" Q0 "+paragraphid+" "+searchRank + " "+searchScore+" Lucene-BM25");
                }
            }
        }
        catch (FileNotFoundException e) {
            System.err.printf("Failed to open input file %s\n", documentCBOR);
            return 1;
        }catch (IOException e){
            e.printStackTrace();
        } catch (ParseException e) {
            
            e.printStackTrace();
        }
    
    
        return 0;
        
    }


    public static int benchquery(final String indexPath, Scanner scanner)
    {
        boolean valid = true;
        while(valid) {
        System.out.println("Enter queryid: ");
        try {
        String qid = scanner.nextLine();
        System.out.println("Enter query: ");
        String qs = scanner.nextLine();
        System.out.println("Input: " + qid + " "+ qs);
        QueryParser parser = new QueryParser("text",new StandardAnalyzer());
        Query query = parser.parse(qs);

        Directory dir = FSDirectory.open(Paths.get(indexPath));
        IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setSimilarity(new BM25Similarity());

            TopDocs hits = searcher.search(query,100);
            System.out.println("\nquery: "+ query + "");
            System.out.println("Total Results :: " + hits.totalHits);
            ScoreDoc[] scoreDocs = hits.scoreDocs;
            for (int i = 0; i < scoreDocs.length; i++) {
                ScoreDoc score = scoreDocs[i];
                final Document doc = searcher.doc(score.doc); 
                final String paragraphid = doc.getField("paragraphid").stringValue();
                final float searchScore = score.score;
                final int searchRank = i+1;
                System.out.println(qid+" Q0 "+paragraphid+" "+searchRank + " "+searchScore+" Lucene-BM25");
            }
        } catch (IOException e){
            e.printStackTrace();
        } catch (ParseException e) {
            
            e.printStackTrace();
        }
        }
        return 0;
    }

  
}