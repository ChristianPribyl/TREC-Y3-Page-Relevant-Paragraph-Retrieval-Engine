package com.TeamHotel.merge_queries;

import com.TeamHotel.inverindex.*;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
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
		System.err.printf("Getting postings lists for %d terms\n", queryTerms.size());
		queryTerms.forEach((term, num) -> {
			Optional<PostingsList> postings = index.getPostingsListForQuery(term, documents);
			if (postings.isPresent()) {
				System.err.printf("Postings for %s has length %d\n", term, postings.get().size());
				postingsLists.add(postings.get());
			} else {
				System.err.printf("Term %s has no postings\n", term);
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

	public static List<Pair<String, Double>> queryJelinekMercer(Index index, Map<String, Integer> queryTerms, double beta, FileWriter logfile, int maxResults) {
			TreeSet<PostingsList> postingsLists = new TreeSet<>((PostingsList l, PostingsList r) -> {
				if (l.size() > r.size()) return 1;
				else if (l.size() < r.size()) return -1;
				else return l.term().compareTo(r.term());
			});
			Map<String, IndexDocument> documents = new TreeMap<>();
			System.err.printf("Getting postings lists for %d terms\n", queryTerms.size());
			queryTerms.forEach((term, num) -> {
				Optional<PostingsList> postings = index.getPostingsListForQuery(term, documents);
				if (postings.isPresent()) {
					System.err.printf("Postings for %s has length %d\n", term, postings.get().size());
					postingsLists.add(postings.get());
				} else {
					System.err.printf("Term %s has no postings\n", term);
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

	public static List<Pair<String, Double>> queryBM25(Index index, Map<String, Integer> queryTerms, int k1, int k2, int k3,
													   double beta, FileWriter logfile, int maxResults) {
		TreeSet<PostingsList> postingsLists = new TreeSet<>((PostingsList l, PostingsList r) -> {
			if (l.size() > r.size()) return 1;
			else if (l.size() < r.size()) return -1;
			else return l.term().compareTo(r.term());
		});
		Map<String, IndexDocument> documents = new TreeMap<>();
		System.err.printf("Getting postings lists for %d terms\n", queryTerms.size());
		queryTerms.forEach((term, num) -> {
			Optional<PostingsList> postings = index.getPostingsListForQuery(term, documents);
			if (postings.isPresent()) {
				System.err.printf("Postings for %s has length %d\n", term, postings.get().size());
				postingsLists.add(postings.get());
			} else {
				System.err.printf("Term %s has no postings\n", term);
			}
		});
		final TreeSet<PostingsList> originalPostings = new TreeSet<>((PostingsList l, PostingsList r) -> {
			if (l.size() > r.size()) return 1;
			else if (l.size() < r.size()) return -1;
			else return l.term().compareTo(r.term());
		});
		originalPostings.addAll(postingsLists);
		List<IndexDocument> matches = merge_OR_query(postingsLists);
		Collection<IndexDocument> rankings = Ranker.bm25(originalPostings, matches, index.getNumDocuments(), k1, k2, k3, beta);
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

	public static List<Pair<String, Double>> mergeFacets(List<List<Pair<String, Double>>> facets, final int maxResults) {
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