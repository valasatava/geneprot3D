package org.rcsb.genevariation.io;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.rcsb.genevariation.datastructures.mRNA;

public class TestGenomeDataProvider {
	
	/**
	 * 
	 * @author Yana Valasatava
	 */
	public static List<mRNA> getExonData() {
		List<mRNA> exons = new ArrayList<mRNA>();
		exons.add(new mRNA(2356, 5432));
		exons.add(new mRNA(1233, 3445));
		exons.add(new mRNA(6789, 98652));
		return exons;
	}

	private List<mRNA> testGettingExonBoundariesForPosition(long position) {
		List<mRNA> exons = getExonData();
		return GenomeDataProvider.filterExonsHavingPosition(exons, position);
	}

	/**
	 * Test
	 */
	@Test
	public void testNotIncluded() {
		long position = 5;
		List<mRNA> exons = testGettingExonBoundariesForPosition(position);
		assertEquals(exons.size(), 0);
	}

	/**
	 * Test
	 */
	@Test
	public void testIncludedInOne() {
		long position = 94352;
		List<mRNA> exons = testGettingExonBoundariesForPosition(position);
		assertEquals(exons.get(0).getStart(), 6789);
		assertEquals(exons.get(0).getEnd(), 98652);
	}
	
	/**
	 * Test
	 */
	@Test
	public void testIncludedInTwo() {
		long position = 2356; // border case
		List<mRNA> exons = testGettingExonBoundariesForPosition(position);
		assertEquals(exons.get(0).getStart(), 2356);
		assertEquals(exons.get(0).getEnd(), 5432); 
		assertEquals(exons.get(1).getStart(), 1233);
		assertEquals(exons.get(1).getEnd(), 3445);
	}
}
