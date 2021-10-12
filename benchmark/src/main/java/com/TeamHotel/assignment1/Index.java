package com.TeamHotel.assignment1;

import edu.unh.cs.treccar_v2.Data;
import edu.unh.cs.treccar_v2.Data.Paragraph;
import edu.unh.cs.treccar_v2.read_data.DeserializeData;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.nio.file.FileSystems;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;


public class Index {
    public static boolean createNewIndex(final String documentCBOR, final String index) {
        // adapted from https://github.com/TREMA-UNH/trec-car-tools/blob/master/trec-car-tools-example/src/main/java/edu/unh/cs/TrecCarBuildLuceneIndex.java
        System.setProperty("file.encoding", "UTF-8");
        try {
            final FileInputStream documentIStream  = new FileInputStream(new File(documentCBOR));
            final IndexWriter indexWriter = new IndexWriter(
                FSDirectory.open(
                    FileSystems.getDefault().getPath(index)), 
                new IndexWriterConfig(new StandardAnalyzer()));
            int i = 0;

            // 2.1 Unpack the test200 dataset of the TREC Complex Answer Retrieval (CAR) data with the trec-car-tools reader.
            for (final Iterator<Data.Paragraph> paragraphIterator = DeserializeData.iterParagraphs(documentIStream); paragraphIterator.hasNext();) {
                final Paragraph paragraph = paragraphIterator.next();
                
                final Document doc = new Document();
                
                // 2.2  Extract the full text of paragraphs
                final String content = paragraph.getTextOnly();
                
                // 2.3 provide fulltext of all paragraphs along with the paragraph ID
                doc.add(new TextField("text", content, Field.Store.YES));
                doc.add(new StringField("paragraphid", paragraph.getParaId(), Field.Store.YES));
                

                indexWriter.addDocument(doc);
                if (i % 1000 == 0) { // check how different values impact performance
                    indexWriter.commit();
                }
                i++;
            }
            indexWriter.commit();
            indexWriter.close();

            System.out.printf("Indexed %s to %s\n", documentCBOR, index);
        }
        catch (FileNotFoundException e) {
            System.err.printf("Failed to open input file %s\n", documentCBOR);
            return false;
        }
        catch (IOException e) {
            System.err.printf("IO exception while indexing %s into %s {%s}\n", documentCBOR, index, e.toString());
            return false;
        }
        return true;
    }

    public static boolean dumpIndex(final String index, FileWriter out) {
        try {
            final IndexReader indexReader = DirectoryReader.open(
                FSDirectory.open(FileSystems.getDefault().getPath(index)));
            int numDocs = indexReader.numDocs();
            for (int i = 0; i < numDocs; i++) {
                Document doc = indexReader.document(i, Set.of("text", "paragraphid"));
                System.out.printf("%s\n%s\n", doc.getField("paragraphid").stringValue(), doc.getField("text").stringValue());
                out.write(doc.getField("paragraphid").stringValue() + "\n" +  doc.getField("text").stringValue()+ "\n");
            }
            indexReader.close();
        }
        catch (IOException e) {
            System.err.printf("IOException while attempting to read lucene index %s {%s}\n", index, e.toString());
            return false;
        }

        return true;
    }
}
