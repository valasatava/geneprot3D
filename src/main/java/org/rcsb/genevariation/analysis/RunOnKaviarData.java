package org.rcsb.genevariation.analysis;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.genome.parsers.genename.GeneChromosomePosition;
import org.biojava.nbio.genome.parsers.twobit.TwoBitFacade;
import org.biojava.nbio.genome.util.ChromosomeMappingTools;
import org.rcsb.genevariation.constants.VariantType;
import org.rcsb.genevariation.datastructures.Mutation;
import org.rcsb.genevariation.datastructures.VcfContainer;
import org.rcsb.genevariation.expression.RNApolymerase;
import org.rcsb.genevariation.expression.Ribosome;
import org.rcsb.genevariation.io.DataLocationProvider;
import org.rcsb.genevariation.io.MappingDataProvider;
import org.rcsb.genevariation.mappers.FilterCodingRegion;
import org.rcsb.genevariation.mappers.FilterSNPs;
import org.rcsb.genevariation.mappers.MapToVcfContainer;
import org.rcsb.genevariation.parser.GenePredictionsParser;
import org.rcsb.genevariation.utils.SaprkUtils;
import org.rcsb.genevariation.utils.VariationUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RunOnKaviarData {

    private static String filepathVCF = DataLocationProvider.getDataHome() + "vcfs/Kaviar-160204-Public-hg38-trim.vcf";
    private static String filepathParquet = DataLocationProvider.getDataHome() + "parquet/Kaviar-database";

    public static void run() throws Exception {

        long start = System.nanoTime();

        JavaSparkContext sc = SaprkUtils.getSparkContext();
        List<GeneChromosomePosition> transcripts = GenePredictionsParser.getGeneChromosomePositions();
        Broadcast<List<GeneChromosomePosition>> transcriptsBroadcast = sc.broadcast(transcripts);

        Encoder<VcfContainer> vcfContainerEncoder = Encoders.bean(VcfContainer.class);

        SaprkUtils.getSparkSession().read()
                .format("com.databricks.spark.csv")
                .option("header", "false")
                .option("delimiter", "\t")
                .option("comment", "#")
                .load(filepathVCF)
                .flatMap(new MapToVcfContainer(), vcfContainerEncoder)
                .filter(new FilterCodingRegion(transcriptsBroadcast))
                .filter(new FilterSNPs())
                .write().mode(SaveMode.Overwrite).parquet(DataLocationProvider.getDataHome() + "parquet/coding-snps-Kaviar.parquet");

        System.out.println("Done: " + (System.nanoTime() - start) / 1E9 + " sec.");
    }

    public static void writeKaviar() throws IOException {

        long start = System.nanoTime();

        Encoder<VcfContainer> vcfContainerEncoder = Encoders.bean(VcfContainer.class);
        SaprkUtils.getSparkSession().read()
                .format("com.databricks.spark.csv")
                .option("header", "false")
                .option("delimiter", "\t")
                .option("comment", "#")
                .load(filepathVCF)
                .flatMap(new MapToVcfContainer(), vcfContainerEncoder)
                .write().mode(SaveMode.Overwrite).parquet(DataLocationProvider.getDataHome() + "parquet/Kaviar-database.parquet");
        System.out.println("Done: " + (System.nanoTime() - start) / 1E9 + " sec.");
    }

    public static void readKaviar() {

        long start = System.nanoTime();
        Dataset<Row> df = SaprkUtils.getSparkSession().read().parquet(filepathParquet);
        df.createOrReplaceTempView("Kaviar");
        df.show();
        System.out.println("Done: " + (System.nanoTime() - start) / 1E9 + " sec.");
    }

    public static void mapKaviarOnHumanGenome() {

        long start = System.nanoTime();

        Dataset<Row> df = SaprkUtils.getSparkSession().read().parquet(filepathParquet);
        df.persist();

        String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",
                "chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19", "chr20", "chr21", "chr22", "chrX", "chrY"};

        for (String chr : chromosomes) {

            System.out.println("getting the data for the chromosome " + chr);
            Dataset<Row> chrom = MappingDataProvider.readHumanChromosomeMapping(chr);
            Dataset<Row> mapping = df.join(chrom, chrom.col("chromosome").equalTo(df.col("chromosome"))
                    .and(chrom.col("position").equalTo(df.col("position"))));
            mapping.write().mode(SaveMode.Overwrite).parquet(DataLocationProvider.getDataHome() + "parquet/Kaviar-hg-mapping/" + chr);
        }
        System.out.println("Done: " + (System.nanoTime() - start) / 1E9 + " sec.");
    }

    public static void writeKaviarOnHumanGenome() throws AnalysisException {

        Dataset<Row> all = null;
//		String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",
//				"chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",  "chr20", "chr21", "chr22", "chrX", "chrY"};

        String[] chromosomes = {"chr21"};

        for (String chr : chromosomes) {

            String filepath = DataLocationProvider.getDataHome() + "parquet/Kaviar-hg-mapping/" + chr;
            Dataset<Row> mapping = SaprkUtils.getSparkSession().read().parquet(filepath);
            mapping.createOrReplaceTempView("mapping");

            Dataset<Row> coding = SaprkUtils.getSparkSession().sql("select * from mapping where inCoding=true");

            if (all == null) {
                all = coding;
            } else {
                all = all.union(coding);
            }
        }
        all.write().mode(SaveMode.Overwrite).parquet(DataLocationProvider.getDataHome() + "parquet/Kaviar-hg-mapping-coding/");
    }

    public static void mapKaviarToMutations() throws AnalysisException, Exception {

        JavaSparkContext sc = SaprkUtils.getSparkContext();
        List<GeneChromosomePosition> transcripts = GenePredictionsParser.getGeneChromosomePositions();
        Broadcast<List<GeneChromosomePosition>> transcriptsBroadcast = sc.broadcast(transcripts);

//        String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",
//                "chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19", "chr20", "chr21", "chr22", "chrX", "chrY"};

        String[] chromosomes = {"chr21"};
        ChromosomeMappingTools mapper = new ChromosomeMappingTools();

        for (String chr : chromosomes) {

            File f = new File(System.getProperty("user.home")+"/data/genevariation/hg38.2bit");
            TwoBitFacade twoBitFacade = new TwoBitFacade(f);

            String filepath = DataLocationProvider.getDataHome() + "parquet/Kaviar-hg-mapping/" + chr;
            Dataset<Row> mapping = SaprkUtils.getSparkSession().read().parquet(filepath).distinct();

            Dataset<Row> filtered = mapping.filter((FilterFunction<Row>) (Row row) -> {
                if (row.get(4).equals(true)) {
                    return true;
                        }
                return false;
            }).drop(mapping.col("inCoding")).filter(new FilterFunction<Row>() {

                        private static final long serialVersionUID = 8450775587033110761L;

                        @Override
                        public boolean call(Row row) throws Exception {
                            String wildtype = row.get(6).toString();
                            String mutation = row.get(7).toString();
                            if (VariationUtils.checkType(wildtype, mutation).compareTo(VariantType.SNP) == 0) {
                                return true;
                            }
                            return false;
                        }
                    });

            List<Mutation> mutations = new ArrayList<>();
            List<Row> snps = filtered.collectAsList();
            for (Row row :snps) {

                for (GeneChromosomePosition cp : transcripts) {
                    if (cp.getChromosome().equals(row.get(2).toString()) && cp.getGeneName().equals(row.get(0).toString()) && (cp.getCdsStart() <= row.getInt(3) && cp.getCdsEnd() >= row.getInt(3))) {

                        int mRNAPos = ChromosomeMappingTools.getCDSPosForChromosomeCoordinate(row.getInt(3), cp);
                        if (mRNAPos == -1)
                            continue;

                        DNASequence dnaSequence = mapper.getTranscriptDNASequence(twoBitFacade, chr, cp.getExonStarts(), cp.getExonEnds(),
                                cp.getCdsStart(), cp.getCdsEnd(), row.get(8).toString().charAt(0));

                        String transcript = dnaSequence.getSequenceAsString();
                        if (transcript.equals(""))
                            continue;

                        RNApolymerase polymerase = new RNApolymerase();
                        String codon = polymerase.getCodon(mRNAPos, transcript);

                        String mutBase;
                        if (row.get(8).toString().charAt(0) == '+') {
                            mutBase = row.get(7).toString();
                        } else {
                            mutBase = VariationUtils.reverseComplimentaryBase(row.get(7).toString());
                        }

                        String codonM = "";
                        if (row.get(8).toString().charAt(0) == '+') {
                            codonM = VariationUtils.mutateCodonForward(mRNAPos, codon, mutBase);
                        } else {
                            codonM = VariationUtils.mutateCodonReverse(mRNAPos, codon, mutBase);
                        }

                        Mutation mutation = new Mutation();
                        mutation.setChromosomeName(row.get(2).toString());
                        mutation.setGeneBankId(cp.getGenebankId());
                        mutation.setPosition(Long.valueOf(row.get(3).toString()));
                        mutation.setUniProtId(row.get(4).toString());
                        mutation.setUniProtPos(Integer.valueOf(row.get(5).toString()));
                        mutation.setRefAminoAcid(Ribosome.getProteinSequence(codon));
                        mutation.setMutAminoAcid(Ribosome.getProteinSequence(codonM));

                        mutations.add(mutation);
                    }
                }
            }

            Dataset<Row> df = SaprkUtils.getSparkSession().createDataFrame(mutations, Mutation.class);
//            Dataset<Row> chromMut = df.drop(df.col("geneBankId"));
//            chromMut.show();
//            chromMut.collect();
//            chromMut.repartition(500);
//            Dataset<Row> chrm = chromMut.dropDuplicates();
//
//            chrm.show();

//            df.write().mode(SaveMode.Overwrite).parquet(DataLocationProvider.getDataHome() + "parquet/Kaviar-mutations/"+chr);
       }
    }

    public static void readKaviarOnHumanGenome() throws AnalysisException {

        String filepath = DataLocationProvider.getDataHome() + "parquet/Kaviar-hg-mapping-coding.parquet/";
        Dataset<Row> mapping = SaprkUtils.getSparkSession().read().parquet(filepath);
        mapping.createOrReplaceTempView("kaviar");

        mapping.show();
        System.out.println(mapping.count());
    }

    public static void main(String[] args) throws Exception {

        long start = System.nanoTime();

        //writeKaviar();
        //readKaviar();
        //mapKaviarOnHumanGenome();
        //writeKaviarOnHumanGenome();
        mapKaviarToMutations();

        System.out.println("Done: " + (System.nanoTime() - start) / 1E9 + " sec.");
    }
}
