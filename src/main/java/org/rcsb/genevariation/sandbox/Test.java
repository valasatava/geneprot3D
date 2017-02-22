package org.rcsb.genevariation.sandbox;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.genevariation.datastructures.SNP;
import org.rcsb.genevariation.datastructures.Variant;
import org.rcsb.genevariation.io.VariantsDataProvider;
import org.rcsb.genevariation.utils.DataProviderFilterChromosome;
import org.rcsb.genevariation.utils.DataProviderFilterInsertion;
import org.rcsb.genevariation.utils.DataProviderFilterSNP;
import org.rcsb.genevariation.utils.IDataProviderFilter;
import org.rcsb.genevariation.utils.SaprkUtils;

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
		
		testSPAT();
		//testChromosomeSNPFilter("21");
		
		//testSNPFilter();
		//testInsertionFilter();
	}

	public static void testSPAT() {
		List<SPA> all = new ArrayList<>();
		for (int i=1; i<5; i++) {
			 SPA snp = new SPA();
			 all.add(snp);
		}
		
		Dataset<Row> snp = SaprkUtils.getSparkSession().createDataFrame(all, SPA.class);
		snp.show();
	}
	
	public static void test() {
		List<SNP> allSNP = new ArrayList<>();
		for (int i=1; i<5; i++) {
			 SNP snp = new SNP();
			 snp.setChromosome(Integer.toString(i));
			 allSNP.add(snp);
		}
		
		Dataset<Row> snp = SaprkUtils.getSparkSession().createDataFrame(allSNP, SNP.class);
		snp.show();
	}
	
	public static void testChromosomeSNPFilter(String chr) throws IOException {

		VariantsDataProvider vdp = new VariantsDataProvider();
		vdp.readVariantsFromVCF(file);
		
		IDataProviderFilter dataFilterChr = new DataProviderFilterChromosome(chr);
		IDataProviderFilter dataFilterVar = new DataProviderFilterSNP();

		vdp.setVariants(vdp.getVariantsByFilter(dataFilterChr));
		Iterator<Variant> vars = vdp.getVariantsByFilter(dataFilterVar);
		while (vars.hasNext()) {
			Variant snp = vars.next();
			//System.out.println(snp.getChromosome()+" "+snp.getPosition()+" "+snp.getType());
		}
	}
	
	public static void testChromosomeInsertionFilter(String chr) throws IOException {

		VariantsDataProvider vdp = new VariantsDataProvider();
		vdp.readVariantsFromVCF(file);
		
		IDataProviderFilter dataFilterChr = new DataProviderFilterChromosome(chr);
		IDataProviderFilter dataFilterVar = new DataProviderFilterInsertion();

		vdp.setVariants(vdp.getVariantsByFilter(dataFilterChr));
		Iterator<Variant> vars = vdp.getVariantsByFilter(dataFilterVar);
		while (vars.hasNext()) {
			Variant snp = vars.next();
			//System.out.println(snp.getChromosome()+" "+snp.getPosition()+" "+snp.getType());
		}
	}
	
	public static void testChromosomeFilter(String chr) throws IOException {

		VariantsDataProvider vdp = new VariantsDataProvider();
		vdp.readVariantsFromVCF(file);
		
		IDataProviderFilter dataFilter = new DataProviderFilterChromosome(chr);
		Iterator<Variant> vars = vdp.getVariantsByFilter(dataFilter);
		while (vars.hasNext()) {
			Variant snp = vars.next();
			//System.out.println(snp.getChromosome()+" "+snp.getPosition()+" "+snp.getType());
		}
	}
	
	public static void testSNPFilter() throws IOException {

		VariantsDataProvider vdp = new VariantsDataProvider();
		vdp.readVariantsFromVCF(file);
		
		IDataProviderFilter dataFilter = new DataProviderFilterSNP();
		Iterator<Variant> vars = vdp.getVariantsByFilter(dataFilter);
		while (vars.hasNext()) {
			Variant snp = vars.next();
			//System.out.println(snp.getChromosome()+" "+snp.getPosition()+" "+snp.getType());
		}
	}
	
	public static void testInsertionFilter() throws IOException {

		VariantsDataProvider vdp = new VariantsDataProvider();
		vdp.readVariantsFromVCF(file);
		
		IDataProviderFilter dataFilter = new DataProviderFilterInsertion();
		Iterator<Variant> vars = vdp.getVariantsByFilter(dataFilter);
		while (vars.hasNext()) {
			Variant snp = vars.next();
			//System.out.println(snp.getChromosome()+" "+snp.getPosition()+" "+snp.getType());
		}
	}
}
