package org.rcsb.genevariation.sandbox;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

import org.rcsb.genevariation.datastructures.Variant;
import org.rcsb.genevariation.io.VariantsDataProvider;
import org.rcsb.genevariation.utils.DataProviderFilterChromosome;
import org.rcsb.genevariation.utils.DataProviderFilterInsertion;
import org.rcsb.genevariation.utils.DataProviderFilterSNP;
import org.rcsb.genevariation.utils.IDataProviderFilter;

/**
 * Test class
 * 
 * @author Yana Valasatava
 */
public class Test {
	
	private final static String userHome = System.getProperty("user.home");
	private final static String path = userHome + "/data/genevariation/vcfExample.vcf";
	private final static Path file = Paths.get(path);
	
	public static void main(String[] args) throws IOException {
		
		testChromosomeSNPFilter(21);
		
		//testSNPFilter();
		//testInsertionFilter();
	}

	public static void testChromosomeSNPFilter(int chr) throws IOException {

		VariantsDataProvider vdp = new VariantsDataProvider();
		vdp.readVariantsFromVCF(file);
		
		IDataProviderFilter dataFilterChr = new DataProviderFilterChromosome(chr);
		IDataProviderFilter dataFilterVar = new DataProviderFilterSNP();

		vdp.setVariants(vdp.getVariantsByFilter(dataFilterChr));
		Iterator<Variant> vars = vdp.getVariantsByFilter(dataFilterVar);
		while (vars.hasNext()) {
			Variant snp = vars.next();
			System.out.println(snp.getChromosome()+" "+snp.getPosition()+" "+snp.getType());
		}
	}
	
	public static void testChromosomeInsertionFilter(int chr) throws IOException {

		VariantsDataProvider vdp = new VariantsDataProvider();
		vdp.readVariantsFromVCF(file);
		
		IDataProviderFilter dataFilterChr = new DataProviderFilterChromosome(chr);
		IDataProviderFilter dataFilterVar = new DataProviderFilterInsertion();

		vdp.setVariants(vdp.getVariantsByFilter(dataFilterChr));
		Iterator<Variant> vars = vdp.getVariantsByFilter(dataFilterVar);
		while (vars.hasNext()) {
			Variant snp = vars.next();
			System.out.println(snp.getChromosome()+" "+snp.getPosition()+" "+snp.getType());
		}
	}
	
	public static void testChromosomeFilter(int chr) throws IOException {

		VariantsDataProvider vdp = new VariantsDataProvider();
		vdp.readVariantsFromVCF(file);
		
		IDataProviderFilter dataFilter = new DataProviderFilterChromosome(chr);
		Iterator<Variant> vars = vdp.getVariantsByFilter(dataFilter);
		while (vars.hasNext()) {
			Variant snp = vars.next();
			System.out.println(snp.getChromosome()+" "+snp.getPosition()+" "+snp.getType());
		}
	}
	
	public static void testSNPFilter() throws IOException {

		VariantsDataProvider vdp = new VariantsDataProvider();
		vdp.readVariantsFromVCF(file);
		
		IDataProviderFilter dataFilter = new DataProviderFilterSNP();
		Iterator<Variant> vars = vdp.getVariantsByFilter(dataFilter);
		while (vars.hasNext()) {
			Variant snp = vars.next();
			System.out.println(snp.getChromosome()+" "+snp.getPosition()+" "+snp.getType());
		}
	}
	
	public static void testInsertionFilter() throws IOException {

		VariantsDataProvider vdp = new VariantsDataProvider();
		vdp.readVariantsFromVCF(file);
		
		IDataProviderFilter dataFilter = new DataProviderFilterInsertion();
		Iterator<Variant> vars = vdp.getVariantsByFilter(dataFilter);
		while (vars.hasNext()) {
			Variant snp = vars.next();
			System.out.println(snp.getChromosome()+" "+snp.getPosition()+" "+snp.getType());
		}
	}
}
