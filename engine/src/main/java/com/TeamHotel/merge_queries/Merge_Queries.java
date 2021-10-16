package com.TeamHotel.merge_queries;

import com.TeamHotel.inverindex.*;

import java.util.*;
import java.lang.Math;

import com.TeamHotel.preprocessor.Preprocess;
import org.apache.commons.lang3.tuple.Pair;

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
					final PostingsList postingsList = index.getPostingsList(processedTerm);
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

			printResults(ranking);
		} catch (NullPointerException e) {

			e.printStackTrace();
		}

		index.resetScoring();
		return true;
	}

	// invertIndex is a HashMap of postings lists.  Each postingsList is an ordered set where each element has an indexed document
	// and meta-data for that document specific to that postings list (term-frequency / score)
	public static List<String> query(final InvertedIndex index, final Map<String, Integer> queryTerms, final String mergeType, final String tfidfVariant) {
		TreeSet<PostingsList> postingsLists = new TreeSet<>((PostingsList l, PostingsList r) -> {
			if (l.size() > r.size()) return 1;
			else if (l.size() < r.size()) return -1;
			else return l.term().compareTo(r.term());
		});
		queryTerms.forEach((term, num) -> {
			PostingsList postings = index.getPostingsList(term);
			if (postings != null) {
				postingsLists.add(postings);
			}
		});
		final TreeSet<PostingsList> originalPostings = new TreeSet<>((PostingsList l, PostingsList r) -> {
			if (l.size() > r.size()) return 1;
			else if (l.size() < r.size()) return -1;
			else return l.term().compareTo(r.term());
		});
		originalPostings.addAll(postingsLists);
		List<IndexDocument> matches;
		if (mergeType.equals("AND")) {
			matches = merge_AND_query(postingsLists);
		} else if (mergeType.equals("OR")) {
			matches = merge_OR_query(postingsLists);
		} else {
			throw new IllegalArgumentException("mergeType must be \"AND\" or \"OR\"");
		}

		Collection<IndexDocument> rankings = Ranker.tfidf(tfidfVariant, originalPostings, matches, index.numDocuments());
		assert(matches.size() == rankings.size());
		System.out.println("Results (also saved to cbor run file):");
		printResults(rankings);
		ArrayList<String> results = new ArrayList<>();
		int i = 1;
		for (IndexDocument d : rankings) {
			results.add(String.format("%s %d %f TeamHotel-%s", d.getFullId(), i, d.getFinalScore(), mergeType));
			// $paragraphId $rank $score $teamName-$methodName
			if (i == 20) break;
			i++;
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

		while( postingsLists.size() > 1 ) {
			Iterator<PostingsList> it = postingsLists.iterator();
			PostingsList smallest_query = it.next();
			PostingsList second_smallest_query = it.next();

			postingsLists.remove(smallest_query);
			postingsLists.remove(second_smallest_query);
			//System.out.println("Merging lists of sizes: " + sorted_smallest.size() + " and " + sorted_second_smallest.size());
			PostingsList result = a_OR_b(smallest_query, second_smallest_query);
			//System.out.println("The result has a size of " + result.size());
			postingsLists.add(result);
		}
		PostingsList result = postingsLists.iterator().next();
		result.iterator().forEachRemaining(pair -> pair.getRight().updateRelevantPostings(result.term(), pair.getLeft()));
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
}