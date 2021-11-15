package com.TeamHotel.merge_queries;

import com.TeamHotel.inverindex.*;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.TeamHotel.preprocessor.Preprocess;

import org.apache.commons.lang3.concurrent.AtomicInitializer;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

public class Merge_Queries {

	/* Class to merge inverted index
	* Takes in an inverted index, type of merge, and scanner for query input
	*
	*
	 */
	public static boolean merge_inverted_index(final InvertedIndex index,
											final String merge_type,
											final String tfidfVariant,
											final Scanner query_search) {

		//Inverted index Map<Term, List<DocId, InternalId, Fulltext>>
		System.out.printf("Indexed %d terms across %d documents\n", index.numTerms(), index.numDocuments());

		TreeSet<PostingsList> postingsLists = new TreeSet<>((PostingsList l, PostingsList r) -> {
			if (l.size() > r.size()) return 1;
			else if (l.size() < r.size()) return -1;
			else return l.term().compareTo(r.term());
		});
		try {
			System.out.print("Input Query: ");
			if (query_search.hasNextLine()) {
				final String queryString = query_search.nextLine();
				if (queryString.equals("quit") || queryString.equals("exit")) {
					return false;
				}
				final String[] queryTerms = queryString.split(" ");
				System.out.println("Query {" + queryString + "} resulted in Terms: " + Arrays.toString(queryTerms));
				for (String term: queryTerms) {
					final String processedTerm = Preprocess.preprocessWord(term);
					final PostingsList postingsList = null;//index.getPostingsList(processedTerm);
					if (postingsList == null || postingsList.size() == 0) {
						System.out.printf("Query term %s is not in the inverted index\n", term);
						postingsLists.add(new PostingsList(term));
					} else {
						System.out.println("Query Added: " + processedTerm);
						System.out.println("Term-Inverted-Index: " + postingsList.size());
						postingsLists.add(postingsList);
					}
				}
			} else {
				return false;
			}
			final TreeSet<PostingsList> originalPostings = new TreeSet<>((PostingsList l, PostingsList r) -> {
				if (l.size() > r.size()) return 1;
				else if (l.size() < r.size()) return -1;
				else return l.term().compareTo(r.term());
			});
			originalPostings.addAll(postingsLists);
			System.out.printf("orig %d done %d\n", originalPostings.size(), postingsLists.size());

			final List<IndexDocument> matches;
			if( merge_type.equals("AND") ) {
				matches = merge_AND_query(postingsLists);

				System.out.println("Results");
			}
			else if( merge_type.equals("OR")) {
				matches = merge_OR_query(postingsLists);

				System.out.println("Results");
			} else {
				matches = new ArrayList<>();
			}
			System.out.printf("orig %d done %d\n", originalPostings.size(), postingsLists.size());

			final Collection<IndexDocument> ranking = Ranker.tfidf(tfidfVariant, originalPostings, matches, index.numDocuments());
			System.out.printf("orig %d done %d\n", originalPostings.size(), postingsLists.size());

			//printResults(ranking);
		} catch (NullPointerException e) {

			e.printStackTrace();
		}

		index.resetScoring();
		return true;
	}

	// invertIndex is a HashMap of postings lists.  Each postingsList is an ordered set where each element has an indexed document
	// and meta-data for that document specific to that postings list (term-frequency / score)
	public static List<Pair<String, Double>> query(@NotNull final Index index, @NotNull final Map<String, Integer> queryTerms, @NotNull final String mergeType, @NotNull final String tfidfVariant, @NotNull final FileWriter logfile) {
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
		List<IndexDocument> matches;
		//System.err.printf("Merging %d postings lists\n", originalPostings.size());
		if (mergeType.equals("AND")) {
			//System.err.println("AND");
			matches = merge_AND_query(postingsLists);
		} else if (mergeType.equals("OR")) {
			//System.err.println("OR");
			matches = merge_OR_query(postingsLists);
		} else {
			throw new IllegalArgumentException("mergeType must be \"AND\" or \"OR\"");
		}
		//System.err.printf("The result contains %d documents\n", matches.size());

		//matches = matches.subList(0, Math.min(5000, matches.size()));
		//System.err.printf("And that gets truncated to %d documents\n", matches.size());

		Collection<IndexDocument> rankings = Ranker.tfidf(tfidfVariant, originalPostings, matches, index.getNumDocuments());
		assert(matches.size() == rankings.size());
		//System.out.println("Results (also saved to cbor run file):");
		//printResults(rankings);
		ArrayList<Pair<String, Double>> results = new ArrayList<>();
		int i = 1;
		for (IndexDocument d : rankings) {
			//results.add(String.format("%s %d %f TeamHotel-%s", d.getFullId(), i, d.getFinalScore(), mergeType));
			results.add(Pair.of(d.getFullId(), d.getFinalScore()));
			// $paragraphId $rank $score $teamName-$methodName
			final Optional<String> fulltext = index.getFulltextById(d.getFullId());
			if (fulltext.isPresent()) {
				try {
					logfile.write(String.format("\n%s: %f/%d: {%s}\n\n", d.getFullId(), d.getFinalScore(), i, fulltext.get()));
				} catch (IOException ex) {
					ex.printStackTrace();
				}
			} else {
				System.out.println("Result without fulltext");
			}
			i++;
			if (i == 21) break;
		}
		return results;
	}

	public static List<IndexDocument> merge_AND_query(TreeSet<PostingsList> postingsLists) {
		if (postingsLists.size() == 0) {
			return new ArrayList<>();
		}

		while( postingsLists.size() > 1 ) {
			Iterator<PostingsList> it = postingsLists.iterator();
			PostingsList smallest_query = it.next();
			PostingsList second_smallest_query = it.next();

			postingsLists.remove(smallest_query);
			postingsLists.remove(second_smallest_query);
			//System.out.println("Merging lists of sizes: " + sorted_smallest.size() + " and " + sorted_second_smallest.size());
			PostingsList result = a_AND_b(smallest_query, second_smallest_query);
			//System.out.println("The result has a size of " + result.size());
			postingsLists.add(result);
		}
		PostingsList result = postingsLists.iterator().next();
		result.iterator().forEachRemaining(pair -> pair.getRight().updateRelevantPostings(result.term(), pair.getLeft()));
		return result.documents();
	}

	public static List<IndexDocument> merge_OR_query(TreeSet<PostingsList> postingsLists) {
		if (postingsLists.isEmpty()) {
			return new ArrayList<>();
		}

		PostingsList result = new PostingsList("");
		postingsLists.forEach(postings -> result.mergeWith(postings));
		/*while( postingsLists.size() > 1 ) {
			Iterator<PostingsList> it = postingsLists.iterator();
			PostingsList smallest_query = it.next();
			PostingsList second_smallest_query = it.next();

			postingsLists.remove(smallest_query);
			postingsLists.remove(second_smallest_query);
			//System.out.println("Merging lists of sizes: " + sorted_smallest.size() + " and " + sorted_second_smallest.size());
			//PostingsList result = a_OR_b(smallest_query, second_smallest_query);
			//System.out.println("The result has a size of " + result.size());
			smallest_query.mergeWith(second_smallest_query);
			postingsLists.add(smallest_query);
		}
		PostingsList result = postingsLists.iterator().next();
		result.iterator().forEachRemaining(pair -> pair.getRight().updateRelevantPostings(result.term(), pair.getLeft()));
		*/
		return result.documents();
	}

	private static PostingsList a_AND_b(PostingsList a, PostingsList b) {
		System.out.printf("a_AND_b called with sizes: %d and %d\n", a.size(), b.size());
		if (a.size() == 0 || b.size() == 0) {
			return new PostingsList(a.term());
		}
		// pretend this function is performing an operation on A.
		// We are adding the tf data of postings list b into the docs in the resulting postings list.
		// To repeat code, each call to a_AND_b will add the tf data from one PostingsList to the documents.
		// By returning a modified postings list for A, we ensure its tf data is preserved for the next call.
		PostingsList result = new PostingsList(a.term());

		Iterator<Pair<IndexIdentifier, IndexDocument>> aIt = a.iterator();
		Iterator<Pair<IndexIdentifier, IndexDocument>> bIt = b.iterator();
		Pair<IndexIdentifier, IndexDocument> aCurr = aIt.next();
		Pair<IndexIdentifier, IndexDocument> bCurr = bIt.next();
		boolean nextA = false;
		boolean nextB = false;
		String aScore = score(aCurr);
		String bScore = score(bCurr);
		//System.out.printf("A: parsing docId %s with score %f\n", aCurr.getRight().getFullId(), aCurr.getRight().getInternalId());
		//System.out.printf("B: parsing docId %s with score %f\n", bCurr.getRight().getFullId(), bCurr.getRight().getInternalId());

		while ((aIt.hasNext() || !nextA) && (bIt.hasNext() || !nextB)) {
			if (nextA) {
				aCurr = aIt.next();
				nextA = false;
				aScore = score(aCurr);
				//System.out.printf("A: parsing docId %s with score %.3f\n", aCurr.getRight().getFullId(), aCurr.getRight().getInternalId());
			}
			if (nextB) {
				bCurr = bIt.next();
				nextB = false;
				bScore = score(bCurr);
				//System.out.printf("B: parsing docId %s with score %.3f\n", bCurr.getRight().getFullId(), bCurr.getRight().getInternalId());
			}
			if (Objects.equals(aScore, bScore)) {
				//System.out.println("Found a match");
				// we collect the tf in each doc values to later compute the tf-idf score
				aCurr.getRight().updateRelevantPostings(b.term(), bCurr.getLeft());

				result.add(aCurr.getLeft(), aCurr.getRight());
				nextA = true;
				nextB = true;
			} else if (aScore.compareTo(bScore) > 0) {
				nextA = true;
			} else {
				nextB = true;
			}
		}
		return result;
	}

	private static PostingsList a_OR_b(PostingsList a, PostingsList b) {
		System.err.printf("%d OR %d -> ", a.size(), b.size());
		if (a.size() == 0 && b.size() == 0) {
			return new PostingsList(a.term());
		}
		if (a.size() == 0) {
			return b;
		}
		if (b.size() == 0) {
			return a;
		}
		// pretend this function is performing an operation on A.
		// We are adding the tf data of postings list b into the docs in the resulting postings list.
		// To repeat code, each call to a_AND_b will add the tf data from one PostingsList to the documents.
		// By returning a modified postings list for A, we ensure its tf data is preserved for the next call.
		PostingsList result = new PostingsList(a.term());

		Iterator<Pair<IndexIdentifier, IndexDocument>> aIt = a.iterator();
		Iterator<Pair<IndexIdentifier, IndexDocument>> bIt = b.iterator();
		Pair<IndexIdentifier, IndexDocument> aCurr = aIt.next();
		Pair<IndexIdentifier, IndexDocument> bCurr = bIt.next();
		boolean nextA = false;
		boolean nextB = false;
		String aScore = score(aCurr);
		String bScore = score(bCurr);
		boolean advanced = true;
		while ((aIt.hasNext() || !nextA) || (bIt.hasNext() || !nextB)) {
			if (nextA && aIt.hasNext()) {
				aCurr = aIt.next();
				nextA = false;
				aScore = score(aCurr);
				advanced = true;
			}
			if (nextB && bIt.hasNext()) {
				bCurr = bIt.next();
				nextB = false;
				bScore = score(bCurr);
				advanced = true;
			}
			if (Objects.equals(aScore, bScore)) {
				// we collect the tf in each doc values to later compute the tf-idf score
				aCurr.getRight().updateRelevantPostings(b.term(), bCurr.getLeft());

				result.add(aCurr.getLeft(), aCurr.getRight());
				nextA = true;
				nextB = true;
			} else if (aScore.compareTo(bScore) > 0) {
				result.add(aCurr.getLeft(), aCurr.getRight());
				nextA = true;
			} else {
				bCurr.getRight().updateRelevantPostings(b.term(), bCurr.getLeft());
				result.add(IndexIdentifier.placeholder(bCurr.getRight().getFullId()), bCurr.getRight());
				nextB = true;
			}
			if (!advanced) {
				break;
			}
			advanced = false;
		}
		System.err.printf("%d\n", result.size());
		return result;
	}

	public static String score(Pair<IndexIdentifier, IndexDocument> posting) {
		// Later when we implement ranking this will change.
		return posting.getRight().getFullId();
	}

	public static void printResults(Collection<IndexDocument> results) {
		results.forEach(d -> System.out.printf("Score %.3f Doc %s (%s)\n\n", d.getFinalScore(),
				d.getFullId(),
				//d.getFulltext().substring(0, Math.min(d.getFulltext().length(), 80))));
				d.getFulltext()));
	}

	public static List<Pair<String, Double>> mergeFacets(List<List<Pair<String, Double>>> facets) {
		//System.err.printf("Merging %d facets\n", facets.size());
		//facets.forEach(l -> {
		//	System.err.printf("Facet has %d results\n", l.size());
		//});
		Map<String, Double> results = new TreeMap<>();
	
		boolean more = true;
		int i = 0;
		while (results.size() < 20 && more) {
			more = false;
			for (List<Pair<String, Double>> facet: facets) {
				if (facet.size() > i && results.size() < 20) {
					Pair<String, Double> result = facet.get(i);
					results.put(result.getLeft(), result.getRight() / (i+1));
					more = true;
				}
			}
			i++;
		}

		List<Pair<String, Double>> res = results.entrySet().stream()
		.map((Map.Entry<String, Double> e) -> Pair.of(e.getKey(), e.getValue()))
		.sorted((Pair<String, Double> l, Pair<String, Double> r) -> Double.compare(l.getRight(), r.getRight()))
		.collect(Collectors.toList());

		if (res.isEmpty() || res.size() == 1) {
			return res;
		} else if (res.get(0).getRight() > res.get(1).getRight()) {
			return res;
		} else {
			//System.err.println("Needed to reverse results");
			Collections.reverse(res);
			return res;
		}
	}
}