package org.rcsb.genevariation.sandbox;

import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.rcsb.genevariation.constants.StrandOrientation;
import org.rcsb.genevariation.datastructures.Mutation;
import org.rcsb.genevariation.datastructures.Transcript;
import org.rcsb.genevariation.datastructures.Variant;
import org.rcsb.genevariation.expression.RNApolymerase;
import org.rcsb.genevariation.expression.Ribosome;
import org.rcsb.genevariation.filters.DataProviderFilterChromosome;
import org.rcsb.genevariation.filters.DataProviderFilterSNP;
import org.rcsb.genevariation.filters.IDataProviderFilter;
import org.rcsb.genevariation.io.VariantsDataProvider;
import org.rcsb.genevariation.parser.GenePredictionsParser;
import org.rcsb.genevariation.utils.SaprkUtils;
import org.rcsb.genevariation.utils.VariationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessDataVCF {

	private final static String userHome = System.getProperty("user.home");
	private final static String variationDataPath = userHome + "/data/genevariation/common_and_clinical_20170130.vcf";

	private static final Logger logger = LoggerFactory.getLogger(ProcessDataVCF.class);

	public static List<Mutation> getMutations(String chrName, VariantsDataProvider vdp) throws Exception {

		// --> GET VARIANTS
		long start2 = System.nanoTime();
		Iterator<Variant> variations = vdp.getAllVariants();
		System.out.println("Time to filter the variation data: " + (System.nanoTime() - start2) / 1E9 + " sec.");

		// --> GET CHROMOSOME POSITIONS
		long start3 = System.nanoTime();
		List<Transcript> transcripts = GenePredictionsParser.getChromosomeMappings().stream()
				.filter(t -> t.getChromosomeName().equals(chrName)).collect(Collectors.toList());
		System.out.println("Time to get chromosome data for "+chrName+": " + (System.nanoTime() - start3) / 1E9 + " sec.");

		// --> MAP GENOMIC COORDINATE TO mRNA POSITION

		long start4 = System.nanoTime();

		List<Mutation> allMutations = new ArrayList<>();
		RNApolymerase polymerase = new RNApolymerase(chrName);
		while (variations.hasNext()) {

			Variant variant = variations.next();

			for (Transcript transcript : transcripts) {

				if ( ( variant.getPosition() >= transcript.getCodingStart() ) && (variant.getPosition() <= transcript.getCodingEnd()) ) {

					int mRNApos = polymerase.getmRNAPositionForGeneticCoordinate((int) variant.getPosition(), transcript);
					if (mRNApos == -1)
						continue;
					
					String codingSequence = polymerase.getCodingSequence(transcript);
					String codon = polymerase.getCodon(mRNApos, codingSequence);

					String mutBase = variant.getAltBase();
					String codonM="";
					if (transcript.getOrientation().equals(StrandOrientation.FORWARD)) {
						codonM = VariationUtils.mutateCodonForward(mRNApos, codon, mutBase);
					}
					else {
						codonM = VariationUtils.mutateCodonReverse(mRNApos, codon, mutBase);
					}

					Mutation mutation = new Mutation();
					mutation.setChromosomeName(chrName);
					mutation.setPosition(variant.getPosition());
					mutation.setRefAA(Ribosome.getCodingAminoAcid(codon));
					mutation.setMutAA(Ribosome.getCodingAminoAcid(codonM));
					allMutations.add(mutation);
				}
			}
		}
		// --> THE END
		System.out.println("Time to map chromosome coordinates to mRNA positions: " + (System.nanoTime() - start4) / 1E9 + " sec.");
		return allMutations;
	}

	public static void main(String[] args) throws Exception {

		logger.info("Started...");
		String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
		System.out.println("Job ID:" + timeStamp);
		long start = System.nanoTime();
		
		List<Mutation> allMutations = new ArrayList<>();

		String[] chromosomes = {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11",  "12", "13", "14", "15", "16", "17", "18", "19",  "20", "21", "22", "X", "Y"};
		String chrName;
		for (String chr : chromosomes) {
			
			chrName = "chr"+chr;
			
			// --> READ VCF FILE
			VariantsDataProvider vdp = new VariantsDataProvider();
			vdp.readVariantsFromVCF(Paths.get(variationDataPath));
			IDataProviderFilter dataFilterChr = new DataProviderFilterChromosome(chrName);
			IDataProviderFilter dataFilterVar = new DataProviderFilterSNP();
			vdp.setVariants(vdp.getVariantsByFilter(dataFilterChr));
			vdp.setVariants(vdp.getVariantsByFilter(dataFilterVar));
			System.out.println("Time to read VCF file: " + (System.nanoTime() - start) / 1E9 + " sec.");
			
			List<Mutation> mutations = getMutations(chrName, vdp);
			allMutations.addAll(mutations);
		}

		Dataset<Row> mydf = SaprkUtils.getSparkSession().createDataFrame(allMutations, Mutation.class);
		mydf.write().mode(SaveMode.Overwrite).parquet(userHome + "/data/genevariation/mutations");

		System.out.println("DONE!");
		System.out.println("Total time: " + (System.nanoTime() - start) / 1E9 + " sec.");
	}
}
