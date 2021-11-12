package com.TeamHotel.main;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import com.TeamHotel.inverindex.Index;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.jetbrains.annotations.NotNull;

public class Clustering {

    /**
     * clusterDocuments should use a clustering algorithm to assign each document to a cluster.
     * @param idx - The index containing each of the documents.  Cluster info should be added to this index using the provided functions.
     * @param maxClusteringRepititions - The max number of repititions of the clustering algorithm to use.  Using fewer is also fine.
     * @param maxDocuments
     * @param offset
     * @return 
     */
    public static int clusterDocuments(@NotNull final Index idx, int maxClusteringRepititions, int offset, int maxDocuments) {
        // example code that might be helpful.
        //for now test 2
        int numclusters = 2;
        idx.clearLeaders();
        //int num = idx.getNumDocuments();
        for (int i = 0; i < numclusters;i++)
        {
            int randomNum = RandomUtils.nextInt(0, maxDocuments);
            Optional<Pair<String, ArrayList<Double>>> randomDocIdAndVector = idx.getDocumentVectorByIndex(randomNum);
            System.out.println("clusterId: " + randomDocIdAndVector.get());
            //idx.setDocumentClass(randomDocIdAndVector.get().getKey(), i);
            idx.addFakeLeader(i + 1, randomDocIdAndVector.get().getValue());
        }
        Iterator<Pair<Integer, ArrayList<Double>>> leaderIterator = idx.getClusterLeaders();

        Iterator<Triple<String, Integer, ArrayList<Double>>> documentIterator = idx.getAllDocuments(0, maxDocuments);
        while( documentIterator.hasNext() ) {
            System.out.println(documentIterator.next().getLeft());
            // EuclideanDistance
            // update the cluser for every document in
            
            
            
        }
        //idx.clearLeaders();


        //Iterator<Pair<Integer, ArrayList<Double>>> leaderIterator = idx.getClusterLeaders();
        //Pair<Integer, ArrayList<Double>> leader = leaderIterator.next();
        //System.out.println("clusterId: " + leader.getLeft().toString() + " " + leader.getRight().toString());
        //System.out.println("clusterId: " + leaderIterator.next().getLeft().toString() + " " + leaderIterator.next().getRight().toString());
        //Iterator<Integer> clusterIterator = idx.getClusters();
        //System.out.println("clusterId: " + clusterIterator.next().intValue());
        //Iterator<Pair<String, ArrayList<Double>>> documentsInCluster = idx.getDocumentsInCluster(1);
        //Pair<String, ArrayList<Double>> document = documentsInCluster.next();
        //System.out.println("clusterId: " + document.getLeft() + " " + document.getRight());
        //idx.clearLeaders();


        //System.out.println("clusterId: " + clusterIterator.next().intValue());
        //System.out.println("clusterId: " + clusterIterator.next().intValue());
        //int clusterId = 1;
        
        //Iterator<Integer> clusterIterator = idx.getClusters();
        //Iterator<Pair<Integer, ArrayList<Double>>> leaderIterator = idx.getClusterLeaders();
        //System.out.println("clusterId: " + clusterIterator.next());
        //Iterator<Integer> clusterIterator = idx.getClusters();
        //Iterator<Pair<String, ArrayList<Double>>> documentsInCluster = idx.getDocumentsInCluster(clusterId); // maximum documents to retrieve
        //Iterator<Triple<String, Integer, ArrayList<Double>>> documentIterator = idx.getAllDocuments(0, 100000000); // documents to skip (offset), max documents to retrieve

        /*
        ArrayList<Double> docVector = new ArrayList<>(WordSimilarity.numDimensions);

        idx.addFakeLeader(clusterId, docVector);
        //Iterator<Integer> clusterIterator = idx.getClusters();
        Iterator<Pair<Integer, ArrayList<Double>>> leaderIterator = idx.getClusterLeaders();
        //System.out.println("clusterId: " + clusterIterator.next());
        System.out.println("clusterId: " + leaderIterator.next().getLeft() + " "+ leaderIterator.next().getRight() );

        //String docId = "asfvwevwtbvqeVQr";
        //int clusterId = 2;
        //idx.setDocumentClass(docId, clusterId);
        */
        //Iterator<Integer> clusterIterator = idx.getClusters();
        //System.out.println("clusterId: " + clusterIterator.); 
        return 0;
        
    }

    /**
     * query 
     * @param idx - The index containing the clustered documents
     * @param query - A list of query terms
     * @param resultsPerQuery - The maximum number of query results.  For TREC-Y3 we can return up to 20 results per query.
     * @return A list of documents and their scores.
     *            The list length should be at most <resultsPerQuery>
     *            The Pair<String, Double> should contain the Document ID, and the Document Score.
     */
    public static List<Pair<String, Double>> query(Index idx, @NotNull final List<String> query, int resultsPerQuery) {
        List<Pair<String, Double>> results = new LinkedList<>();

        // example
        String docId = "rvqWRVAVWetbVWEqw"; // the trec id
        double score = 0.54;
        results.add(Pair.of(docId, score));
        return results;
    }

    public static double EuclideanDistance(ArrayList<Double> p1, ArrayList<Double> p2)
        {
            double sum = 0;
            for (int i = 0; i < p1.size(); i++)
            {
                double d = p1.get(i) - p2.get(i);
                sum += d * d;
            }
            return Math.sqrt(sum);
        }

    public static ArrayList<Double> centroid(ArrayList<Double> p1, ArrayList<Double> p2)
    {
        ArrayList<Double> center = new ArrayList<Double>(p1.size());
        for (int i = 0; i < p1.size(); i++)
        {
            center.set(i,p1.get(i) + p2.get(i));
        
        }
        return center;
    }

}
