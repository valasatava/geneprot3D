package org.rcsb.genevariation.sandbox;

import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.biojava.nbio.genome.parsers.genename.GeneChromosomePosition;
import org.biojava.nbio.genome.util.ChromosomeMappingTools;
import org.rcsb.genevariation.datastructures.Transcript;
import org.rcsb.genevariation.datastructures.Variant;
import org.rcsb.genevariation.expression.RNApolymerase;
import org.rcsb.genevariation.io.GenomeDataProvider;
import org.rcsb.genevariation.io.VariantsDataProvider;
import org.rcsb.genevariation.parser.GenePredictionsParser;
import org.rcsb.genevariation.utils.DataProviderFilterChromosome;
import org.rcsb.genevariation.utils.DataProviderFilterSNP;
import org.rcsb.genevariation.utils.IDataProviderFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessDataVCF {

	private final static String userHome = System.getProperty("user.home");
	private final static String variationDataPath = userHome + "/data/genevariation/common_and_clinical_20170130.vcf";
	
	private static final Logger logger = LoggerFactory.getLogger(ProcessDataVCF.class);

	public static void main(String[] args) throws Exception {
		
		String chrName = "chr14";

		logger.info("Started...");
		String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
		System.out.println("Job ID:" + timeStamp);
		
		// --> READ VCF FILE
		long start = System.nanoTime();
		VariantsDataProvider vdp = new VariantsDataProvider();
		vdp.readVariantsFromVCF(Paths.get(variationDataPath));
		System.out.println("Time to read VCF file: " + (System.nanoTime() - start) / 1E9 + " sec.");

		// --> GET VARIANTS
		long start2 = System.nanoTime();
		IDataProviderFilter dataFilterChr = new DataProviderFilterChromosome(chrName);
		IDataProviderFilter dataFilterVar = new DataProviderFilterSNP();
		vdp.setVariants(vdp.getVariantsByFilter(dataFilterChr));
		vdp.setVariants(vdp.getVariantsByFilter(dataFilterVar));
		Iterator<Variant> variations = vdp.getAllVariants();
		System.out.println("Time to filter the variation data: " + (System.nanoTime() - start2) / 1E9 + " sec.");

		// --> GET CHROMOSOME POSITIONS
		long start3 = System.nanoTime();
		List<Transcript> transcripts = GenePredictionsParser.getChromosomeMappings().stream()
				.filter(t -> t.getChromosomeName().equals(chrName)).collect(Collectors.toList());
		System.out.println("Time to get chromosome data: " + (System.nanoTime() - start3) / 1E9 + " sec.");

		// --> MAP GENOMIC COORDINATE TO mRNA POSITION
		
		long start4 = System.nanoTime();
		
		RNApolymerase rnaPolymerase = new RNApolymerase(chrName);
		while (variations.hasNext()) {

			Variant variant = variations.next();
			for (Transcript transcript : transcripts) {
			
				if ( ( variant.getPosition() >= transcript.getCodingStart() ) && (variant.getPosition() <= transcript.getCodingEnd()) ) {
					
					System.out.println(transcript.getGeneName() + " " + transcript.getGeneBankId());
					System.out.println(transcript.getChromosomeName() + " "  + variant.getRefBase() + " -> "+ variant.getAltBase() + " " 
										+ variant.getPosition());
					
					String codon = rnaPolymerase.getCodingCodon( (int) variant.getPosition(), transcript);

					
					System.out.println();
				}
			}
		}
		
		System.out.println("Time to map chromosome coordinates to mRNA positions: " + (System.nanoTime() - start4) / 1E9 + " sec.");
		
		// --> THE END
		
		System.out.println("DONE!");
		System.out.println("Total time: " + (System.nanoTime() - start) / 1E9 + " sec.");

	}
}
