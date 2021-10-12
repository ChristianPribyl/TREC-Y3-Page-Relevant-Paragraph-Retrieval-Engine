package com.TeamHotel.assignment1;

import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        // TODO improve parameter sanitization
        if (args.length < 2 || args.length > 4) {
            System.out.println("Usage: java -jar Assignment1<version>.jar (index | query | dump-paragraphs)");
            System.out.println("    index <cbor-paragraphs.cbor> <index-out-dir>");
            System.out.println("    dump-paragraph <index-dir>");
            System.out.println("    query <cbor-outline.cbor> <index-dir> -> <filename>.run");
            return;
        }
        
        if (args[0].equals("index"))
        {
            String index =  String.format("%s/index.lucene", args[2]);
            System.out.printf("Indexing documents under %s\n", args[1]);
            Index.createNewIndex(args[1], index);
            System.out.println("Indexing complete");
        }
        else if (args[0].equals("dump-index")) {
            String index =  String.format("%s/index.lucene", args[1]);
            System.out.printf("Dumping lucene index at %s\n", index);
            Index.dumpIndex(index);
            System.out.println("\nFinished dumping index");
        }
        else if (args[0].equals("query")) {
            String index =  String.format("%s/index.lucene", args[2]);
            QueryEngine.rank(args[1],index);
            System.out.println("Ranking complete");
        }else if (args[0].equals("bench-query"))
        {
            String index =  String.format("%s/index.lucene", args[1]);
            Scanner scanner = new Scanner(System.in);
            QueryEngine.benchquery(index, scanner);
            System.out.println("query complete");
        }
        
    }
}