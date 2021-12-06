package com.TeamHotel.merge_queries;

import com.TeamHotel.inverindex.*;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

public class Merge_Queries {
	// invertIndex is a HashMap of postings lists.  Each postingsList is an ordered set where each element has an indexed document
	// and meta-data for that document specific to that postings list (term-frequency / score)
	public static List<Pair<String, Double>> queryTfidf(@NotNull final Index index, @NotNull final Map<String, Integer> queryTerms, @NotNull final String tfidfVariant, @NotNull final FileWriter logfile, final int maxResults) {
		TreeSet<PostingsList> postingsLists = new TreeSet<>((PostingsList l, PostingsList r) -> {
			if (l.size() > r.size()) return 1;
			else if (l.size() < r.size()) return -1;
			else return l.term().compareTo(r.term());
		});
		Map<String, IndexDocument> documents = new TreeMap<>();
		//System.err.printf("Getting postings lists for %d terms\n", queryTerms.size());
		queryTerms.forEach((term, num) -> {
			Optional<PostingsList> postings = index.getPostingsListForQuery(term, documents);
			if (postings.isPresent()) {
				//System.err.printf("Postings for %s has length %d\n", term, postings.get().size());
				postingsLists.add(postings.get());
			} else {
				//System.err.printf("Term %s has no postings\n", term);
			}
		});
		final TreeSet<PostingsList> originalPostings = new TreeSet<>((PostingsList l, PostingsList r) -> {
			if (l.size() > r.size()) return 1;
			else if (l.size() < r.size()) return -1;
			else return l.term().compareTo(r.term());
		});
		originalPostings.addAll(postingsLists);
		List<IndexDocument> matches = merge_OR_query(postingsLists);
		Collection<IndexDocument> rankings = Ranker.tfidf(tfidfVariant, originalPostings, matches, index.getNumDocuments());
		ArrayList<Pair<String, Double>> results = new ArrayList<>();
		int i = 1;
		for (IndexDocument d : rankings) {
			results.add(Pair.of(d.getFullId(), d.getFinalScore()));
			try {
				logfile.write(String.format("\n%s: %f/%d: {%s}\n\n", d.getFullId(), d.getFinalScore(), i, index.getFulltextById(d.getFullId()).get()));
			} catch (IOException ex) {
				ex.printStackTrace();
			}
			if (i == maxResults) break;
			i++;
		}
		return results;
	}

	public static List<Pair<String, Double>> queryBM25(Index index, Map<String, Integer> queryTerms, double k1, double k3,
		double beta, FileWriter logfile, int maxResults) {
		TreeSet<PostingsList> postingsLists = new TreeSet<>((PostingsList l, PostingsList r) -> {
			if (l.size() > r.size()) return 1;
			else if (l.size() < r.size()) return -1;
			else return l.term().compareTo(r.term());
		});
		Map<String, IndexDocument> documents = new TreeMap<>();
		//System.err.printf("Getting postings lists for %d terms\n", queryTerms.size());
		queryTerms.forEach((term, num) -> {
			Optional<PostingsList> postings = index.getPostingsListForQuery(term, documents);
			if (postings.isPresent()) {
				//System.err.printf("Postings for %s has length %d\n", term, postings.get().size());
				postingsLists.add(postings.get());
			} else {
				//System.err.printf("Term %s has no postings\n", term);
			}
		});
		final TreeSet<PostingsList> originalPostings = new TreeSet<>((PostingsList l, PostingsList r) -> {
			if (l.size() > r.size()) return 1;
			else if (l.size() < r.size()) return -1;
			else return l.term().compareTo(r.term());
		});
		originalPostings.addAll(postingsLists);
		List<IndexDocument> matches = merge_OR_query(postingsLists);
		Collection<IndexDocument> rankings = Ranker.bm25(originalPostings, matches, index.getNumDocuments(), k1, k3, beta);
		ArrayList<Pair<String, Double>> results = new ArrayList<>();
		int i = 1;
		for (IndexDocument d : rankings) {
			results.add(Pair.of(d.getFullId(), d.getFinalScore()));
			try {
				logfile.write(String.format("\n%s: %f/%d: {%s}\n\n", d.getFullId(), d.getFinalScore(), i, index.getFulltextById(d.getFullId()).get()));
			} catch (IOException ex) {
				ex.printStackTrace();
			}
			if (i == maxResults) break;
			i++;
		}
		return results;
}

	public static List<Pair<String, Double>> queryJelinekMercer(Index index, Map<String, Integer> queryTerms, double beta, FileWriter logfile, int maxResults) {
			TreeSet<PostingsList> postingsLists = new TreeSet<>((PostingsList l, PostingsList r) -> {
				if (l.size() > r.size()) return 1;
				else if (l.size() < r.size()) return -1;
				else return l.term().compareTo(r.term());
			});
			Map<String, IndexDocument> documents = new TreeMap<>();
			//System.err.printf("Getting postings lists for %d terms\n", queryTerms.size());
			queryTerms.forEach((term, num) -> {
				Optional<PostingsList> postings = index.getPostingsListForQuery(term, documents);
				if (postings.isPresent()) {
					//System.err.printf("Postings for %s has length %d\n", term, postings.get().size());
					postingsLists.add(postings.get());
				} else {
					//System.err.printf("Term %s has no postings\n", term);
				}
			});
			final TreeSet<PostingsList> originalPostings = new TreeSet<>((PostingsList l, PostingsList r) -> {
				if (l.size() > r.size()) return 1;
				else if (l.size() < r.size()) return -1;
				else return l.term().compareTo(r.term());
			});
			originalPostings.addAll(postingsLists);
			List<IndexDocument> matches = merge_OR_query(postingsLists);
			Collection<IndexDocument> rankings = Ranker.jelinekMercer(originalPostings, matches, index.getNumDocuments(), beta);
			ArrayList<Pair<String, Double>> results = new ArrayList<>();
			int i = 1;
			for (IndexDocument d : rankings) {
				results.add(Pair.of(d.getFullId(), d.getFinalScore()));
				try {
					logfile.write(String.format("\n%s: %f/%d: {%s}\n\n", d.getFullId(), d.getFinalScore(), i, index.getFulltextById(d.getFullId()).get()));
				} catch (IOException ex) {
					ex.printStackTrace();
				}
				if (i == maxResults) break;
				i++;
			}
			return results;
	}

	public static List<Pair<String, Double>> queryBIM(Index index, Map<String, Integer> queryTerms,double a, double b, FileWriter logfile ,int maxResults) {
			TreeSet<PostingsList> postingsLists = new TreeSet<>((PostingsList l, PostingsList r) -> {
				if (l.size() > r.size()) return 1;
				else if (l.size() < r.size()) return -1;
				else return l.term().compareTo(r.term());
			});
			Map<String, IndexDocument> documents = new TreeMap<>();
			//System.err.printf("Getting postings lists for %d terms\n", queryTerms.size());
			queryTerms.forEach((term, num) -> {
				Optional<PostingsList> postings = index.getPostingsListForQuery(term, documents);
				if (postings.isPresent()) {
					//System.err.printf("Postings for %s has length %d\n", term, postings.get().size());
					postingsLists.add(postings.get());
				} else {
					//System.err.printf("Term %s has no postings\n", term);
				}
			});
			final TreeSet<PostingsList> originalPostings = new TreeSet<>((PostingsList l, PostingsList r) -> {
				if (l.size() > r.size()) return 1;
				else if (l.size() < r.size()) return -1;
				else return l.term().compareTo(r.term());
			});
			originalPostings.addAll(postingsLists);
			List<IndexDocument> matches = merge_OR_query(postingsLists);
		
			Collection<IndexDocument> rankings = Ranker.bim(originalPostings, matches, index.getNumDocuments(), a,b);
			ArrayList<Pair<String, Double>> results = new ArrayList<>();
			int i = 1;
			for (IndexDocument d : rankings) {
				results.add(Pair.of(d.getFullId(), d.getFinalScore()));
				try {
					logfile.write(String.format("\n%s: %f/%d: {%s}\n\n", d.getFullId(), d.getFinalScore(), i, index.getFulltextById(d.getFullId()).get()));
				} catch (IOException ex) {
					ex.printStackTrace();
				}
				if (i == maxResults) break;
				i++;
			}
			
			return results;
	}





	public static List<IndexDocument> merge_OR_query(TreeSet<PostingsList> postingsLists) {
		if (postingsLists.isEmpty()) {
			return new ArrayList<>();
		}

		PostingsList result = new PostingsList("");
		postingsLists.forEach(postings -> result.mergeWith(postings));
		return result.documents();
	}

	public static List<Pair<String, Double>> mergeFacets(List<List<Pair<String, Double>>> facets, final String mergeType, final int maxResults) {
		if (mergeType.equalsIgnoreCase("RoundRobin")) {
			return mergeRoundRobin(facets, maxResults);
		} else if (mergeType.equalsIgnoreCase("RankRecurrance")) {
			return mergeWeighRecurringResults(facets, maxResults);
		} else if (mergeType.equalsIgnoreCase("Recurrance")) {
			return mergeByRecurrance(facets, maxResults);
		} else {
			return null;
		}
	}

	public static List<Pair<String, Double>> mergeRoundRobin(List<List<Pair<String, Double>>> facets, final int maxResults) {
		//List<Pair<String, Double>> results = new ArrayList<>(maxResults);
		Map<String, Double> results = new TreeMap<>();

		List<Iterator<Pair<String, Double>>> its = new LinkedList<>();
		for (List<Pair<String, Double>> facet: facets) {
			its.add(facet.iterator());
		}
		AtomicInteger i = new AtomicInteger(0);
		AtomicBoolean more = new AtomicBoolean(true);
		while (results.size() < maxResults && more.get()) {
			more.set(false);
			its.forEach(it -> {
				if(it.hasNext() && results.size() < maxResults) {
					Pair<String, Double> doc = it.next();
					if (results.get(doc.getLeft()) == null) {
						results.put(doc.getLeft(), 1.0 * maxResults - i.getAndIncrement());
					}
					more.set(true);
				}
			});
		}
		return results.entrySet().stream()
			.sorted((l, r) -> -Double.compare(l.getValue(), r.getValue()))
			.limit(maxResults)
			.map(e -> Pair.of(e.getKey(), e.getValue()))
			.collect(Collectors.toList());
	}

	// each facet result is scored as 1/rank.  Ranks are summed across all documents then sorted.
	public static List<Pair<String, Double>> mergeWeighRecurringResults(List<List<Pair<String, Double>>> facets, final int maxResults) {
		Map<String, Double> results = new TreeMap<>();
		combineScoresAndSetToRank(facets, maxResults).stream().flatMap(facet -> facet.stream())
		.map(doc -> Pair.of(doc.getLeft(), 1.0 / doc.getRight()))
		.forEach(doc -> {
			Double previousVal = results.putIfAbsent(doc.getLeft(), doc.getRight());
			if (previousVal != null) {
				results.put(doc.getLeft(), doc.getRight() + previousVal);
			}
		});
		
		return results.entrySet().stream()
			.sorted((Map.Entry<String, Double> l, Map.Entry<String, Double> r) -> Double.compare(l.getValue(), r.getValue()))
			.limit(maxResults)
			.map(e -> Pair.of(e.getKey(), e.getValue()))
			.collect(Collectors.toList());
	}

	// sort results by the number of facets that return each document.
	// Break ties by favoring the document that appears soonest in its facets rankings.
	public static List<Pair<String, Double>> mergeByRecurrance(List<List<Pair<String, Double>>> facets, final int maxResults) {
		Map<String, Double> results = new TreeMap<>();
		combineScoresAndSetToRank(facets, maxResults).stream().flatMap(facet -> facet.stream())
		.forEach(doc -> {
			Double previousVal = results.get(doc.getLeft());
			if (previousVal == null) {
				results.put(doc.getLeft(), 1 + doc.getRight());
			} else if (doc.getRight() > previousVal % 1.0) {
				results.put(doc.getLeft(), 1 + (previousVal % 1.0) + doc.getRight());
			} else {
				results.put(doc.getLeft(), 1 + previousVal);
			}
		});
		
		return results.entrySet().stream()
			.sorted((Map.Entry<String, Double> l, Map.Entry<String, Double> r) -> Double.compare(l.getValue(), r.getValue()))
			.limit(maxResults)
			.map(e -> Pair.of(e.getKey(), e.getValue()))
			.collect(Collectors.toList());
	}

	private static List<List<Pair<String, Double>>> combineScoresAndSetToRank(List<List<Pair<String, Double>>> facets, final int maxResults) {
		List<List<Pair<String, Double>>> res = new ArrayList<>(facets.size());
		for (List<Pair<String, Double>> facet: facets) {
			int index = 1;
			List<Pair<String, Double>> newFacet = new ArrayList<>(facet.size());
			for (Pair<String, Double> doc: facet) {
				newFacet.add(Pair.of(doc.getLeft(), 1.0 / index));
				index++;
			}
			res.add(newFacet);
		}
		return facets;
	}

	public static List<Pair<String, Double>> mergeWeightingIncomperableScores(List<List<Pair<String, Double>>> facets, final int maxResults) {
		Map<String, Double> results = new TreeMap<>();
	
		for (List<Pair<String, Double>> facet: facets) {
			Iterator<Pair<String, Double>> it = facet.iterator();
			for (int i = 0; i < maxResults && it.hasNext(); i++) {
				Pair<String, Double> p = it.next();
				results.put(p.getLeft(), p.getRight() / (i + 1));
			}
		}

		List<Pair<String, Double>> res = results.entrySet().stream()
		.map((Map.Entry<String, Double> e) -> Pair.of(e.getKey(), e.getValue()))
		.sorted((Pair<String, Double> l, Pair<String, Double> r) -> -Double.compare(l.getRight(), r.getRight()))
		.limit(maxResults)
		.collect(Collectors.toList());
		return res;
	}

	public static List<Pair<String, Double>> filterUnscored(List<Pair<String, Double>> results, Set<String> scoredDocs, int maxResults) {
		return results.stream().filter(p -> scoredDocs.contains(p.getLeft())).limit(maxResults).collect(Collectors.toList());
	}
}
