package com.TeamHotel.main;

import java.util.ArrayList;
import java.util.Collections;
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
        //for (int i = 0; i < numclusters;i++)
        //{
            //int randomNum = RandomUtils.nextInt(0, maxDocuments);
            Optional<Pair<String, ArrayList<Double>>> randomDocIdAndVector = idx.getDocumentVectorByIndex(28395);
            System.out.println("clusterId: " + randomDocIdAndVector.get());
            //idx.setDocumentClass(randomDocIdAndVector.get().getKey(), i);
            idx.addFakeLeader(1, randomDocIdAndVector.get().getValue());
            Optional<Pair<String, ArrayList<Double>>> randomDocIdAndVector1 = idx.getDocumentVectorByIndex(44146);
            idx.addFakeLeader(2, randomDocIdAndVector1.get().getValue());
            System.out.println("clusterId: " + randomDocIdAndVector.get());
        //}
        updateCluster(idx,maxDocuments );
        idx.clearLeaders();
        System.out.println("Assigning new clusters to each document");
        // loop over Repititions
        for (int i = 0; i < maxClusteringRepititions; i++) {
        System.out.println(i);
            newLeader(idx);
            updateCluster(idx,maxDocuments );
            idx.clearLeaders();
        }
    
    
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

    public static Integer updateCluster (final Index idx, Integer numOfDoc)
    {
        idx.beginTransaction();
        Iterator<Triple<String, Integer, ArrayList<Double>>> documentIterator = idx.getAllDocuments(0, numOfDoc);
        // loop over all documents;
        int numUpdated = 0;
        while( documentIterator.hasNext() ) {
            Iterator<Pair<Integer, ArrayList<Double>>> leaderIterator = idx.getClusterLeaders();
            Triple<String, Integer, ArrayList<Double>> doc = documentIterator.next();
            ArrayList<Double> docvec = doc.getRight();
            String currdocID = doc.getLeft();
            int clusterid = 0;
            double closest = Double.MAX_VALUE;
            // assigned document with near leader cluster id
            System.out.println("Looping over leaders");
            while ( leaderIterator.hasNext() ) {
                Pair<Integer, ArrayList<Double>> leader = leaderIterator.next();
                ArrayList<Double> leaderDocVect = leader.getRight();
                double dist = EuclideanDistance(leaderDocVect,docvec);
                if(dist <= closest)
                {
                    clusterid = leader.getLeft();
                    closest = dist;
                    //System.out.println(clusterid);
                }
            }
            idx.setDocumentClass(currdocID, clusterid);
            numUpdated++;
            if (numUpdated % 10 == 0) {
                System.out.printf("Assigned clusters to %d documents\n", numUpdated);
            }
        }
        idx.commitTransaction();
        
        return 0;
    }

    public static double EuclideanDistance(ArrayList<Double> p1, ArrayList<Double> p2)
        {
            double sum = 0;
            for (int i = 0; i < p1.size(); i++)
            {
                double d = p1.get(i) - p2.get(i);
                sum += d * d;
            }
            return sum;//Math.sqrt(sum);
        }
    

    
    public static Integer newLeader(final Index idx)
    {
        Iterator<Integer> clusterIterator = idx.getClusters();
        //ArrayList<Double> center = new ArrayList<Double>(p1.size());
        
        // loop over clusters. 
        while (clusterIterator.hasNext())
        {
            int clusID = clusterIterator.next();

            Iterator<Pair<String, ArrayList<Double>>> documentsInCluster = idx.getDocumentsInCluster(clusID);
            ArrayList<Double> center = new ArrayList<Double>(Collections.nCopies(WordSimilarity.numDimensions, 0.0));
            int numOfClust = 0;
            //Sum of all docVectoer by clust id and divid numofclust to get centroid
            while (documentsInCluster.hasNext())
            {
                ArrayList<Double> currVec = documentsInCluster.next().getRight();
                sumAll(center,currVec);
                numOfClust++;
            }
            centroid(center,numOfClust);
            idx.addFakeLeader(clusID, center);
        }
        return 0;
    }
    public static ArrayList<Double> sumAll(ArrayList<Double> p1, ArrayList<Double> p2)
    {
    
        ArrayList<Double> sum = new ArrayList<Double>(Collections.nCopies(WordSimilarity.numDimensions, 0.0));
        for (int i = 0; i < p1.size(); i++)
        {
            //System.out.println(i);
            sum.set(i,p1.get(i) + p2.get(i));
        
        }

        return sum;
    }

    public static ArrayList<Double> centroid (ArrayList<Double> p1, Integer num)
    {
        for (int i = 0; i < p1.size(); i++)
        {
            p1.set(i,p1.get(i)/num);
        
        }
        return p1;
    }
}
