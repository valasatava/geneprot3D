package org.rcsb.genevariation.io;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.rcsb.genevariation.datastructures.Exon;

public class TestGenomeDataProvider {
	
	/**
	 * 
	 * @author Yana Valasatava
	 */
	public static List<Exon> getExonData() {
		List<Exon> exons = new ArrayList<Exon>();
		exons.add(new Exon(2356, 5432));
		exons.add(new Exon(1233, 3445));
		exons.add(new Exon(6789, 98652));
		return exons;
	}

	private List<Exon> testGettingExonBoundariesForPosition(long position) {
		List<Exon> exons = getExonData();
		return GenomeDataProvider.filterExonsHavingPosition(exons, position);
	}

	/**
	 * Test
	 */
	@Test
	public void testNotIncluded() {
		long position = 5;
		List<Exon> exons = testGettingExonBoundariesForPosition(position);
		assertEquals(exons.size(), 0);
	}

	/**
	 * Test
	 */
	@Test
	public void testIncludedInOne() {
		long position = 94352;
		List<Exon> exons = testGettingExonBoundariesForPosition(position);
		assertEquals(exons.get(0).getStart(), 6789);
		assertEquals(exons.get(0).getEnd(), 98652);
	}
	
	/**
	 * Test
	 */
	@Test
	public void testIncludedInTwo() {
		long position = 2356; // border case
		List<Exon> exons = testGettingExonBoundariesForPosition(position);
		assertEquals(exons.get(0).getStart(), 2356);
		assertEquals(exons.get(0).getEnd(), 5432); 
		assertEquals(exons.get(1).getStart(), 1233);
		assertEquals(exons.get(1).getEnd(), 3445);
	}
}
