package exonscorrelation;

import exonscorrelation.mappers.MapToProteinDisorder;
import exonscorrelation.utils.ExonsUtils;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.rcsb.genevariation.datastructures.ExonSerializable;
import org.rcsb.genevariation.datastructures.ProteinFeatures;
import org.rcsb.genevariation.io.DataLocationProvider;
import org.rcsb.genevariation.io.MappingDataProvider;
import org.rcsb.genevariation.utils.SaprkUtils;

import java.io.IOException;
import java.util.List;

public class AnalyzeExons {

	private static String path = DataLocationProvider.getExonsProject();

	public static void mapExonsToIsoformPositions(String exonsdatapath, String exonsuniprotpath) {

		Dataset<Row> data = SaprkUtils.getSparkSession().read().parquet(exonsdatapath);
		data.persist(StorageLevel.MEMORY_AND_DISK());

		String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",
				"chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",  "chr20", "chr21", "chr22", "chrX", "chrY"};

//		String[] chromosomes = {"chr21"};

		for (String chr : chromosomes) {

			Dataset<Row> map = MappingDataProvider.readHumanChromosomeMapping(chr);

			Dataset<Row> df1 = data.join(map, data.col("chromosome").equalTo(map.col("chromosome"))
					.and(data.col("geneBankId").equalTo(map.col("geneBankId"))).and(data.col("start").equalTo(map.col("position"))), "inner")
					.drop(map.col("chromosome")).drop(map.col("geneBankId")).drop(map.col("orientation"))
					.drop(map.col("geneSymbol")).drop(map.col("geneName")).drop(map.col("mRNAPos")).drop(map.col("exonNum"))
					.drop(map.col("inCoding")).drop(map.col("inUtr")).drop(map.col("position"))
					.drop(map.col("phase")).withColumnRenamed("uniProtIsoformPos", "isoformPosStart").withColumnRenamed("uniProtCanonicalPos", "canonicalPosStart");

			Dataset<Row> df2 = data.join(map, data.col("chromosome").equalTo(map.col("chromosome"))
					.and(data.col("geneBankId").equalTo(map.col("geneBankId"))).and(data.col("end").equalTo(map.col("position"))), "inner")
					.drop(map.col("chromosome")).drop(map.col("geneBankId")).drop(map.col("orientation"))
					.drop(map.col("geneSymbol")).drop(map.col("geneName")).drop(map.col("mRNAPos")).drop(map.col("exonNum"))
					.drop(map.col("inCoding")).drop(map.col("inUtr")).drop(map.col("position"))
					.drop(map.col("phase")).withColumnRenamed("uniProtIsoformPos", "isoformPosEnd").withColumnRenamed("uniProtCanonicalPos", "canonicalPosEnd");

			Dataset<Row> df = df1.join(df2,
					df1.col("chromosome").equalTo(df2.col("chromosome"))
					.and(df1.col("geneBankId").equalTo(df2.col("geneBankId")))
					.and(df1.col("ensemblId").equalTo(df2.col("ensemblId")))
					.and(df1.col("isoformIndex").equalTo(df2.col("isoformIndex")))
					.and(df1.col("start").equalTo(df2.col("start")))
					.and(df1.col("end").equalTo(df2.col("end"))), "inner")
					.drop(df2.col("chromosome")).drop(df2.col("geneBankId")).drop(df2.col("ensemblId"))
					.drop(df2.col("orientation")).drop(df2.col("offset")).drop(df2.col("geneName"))
					.drop(df2.col("start")).drop(df2.col("end")).drop(df2.col("isoformIndex")).drop(df2.col("uniProtId"));

			Dataset<Row> ordered = df.select("chromosome", "geneName", "ensemblId", "geneBankId", "start", "end", "orientation", "offset",
					"uniProtId", "canonicalPosStart", "canonicalPosEnd", "isoformIndex", "isoformPosStart", "isoformPosEnd").orderBy("start", "isoformIndex");

			ordered.write().mode(SaveMode.Overwrite).parquet(exonsuniprotpath+"/"+chr);
		}
	}

	public static void mapToPDBPositions(String uniprotmapping, String pdbmapping ) {

		Dataset<Row> mapUniprotToPdb = MappingDataProvider.readPdbUniprotMapping();

//		String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",
//				"chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",  "chr20", "chr21", "chr22", "chrX", "chrY"};
		String[] chromosomes = {"chr21"};

		for (String chr : chromosomes) {

			Dataset<Row> mapToUniprot = SaprkUtils.getSparkSession().read().parquet(uniprotmapping+"/"+chr);

			Dataset<Row> mapToPDBStart = mapToUniprot.join(mapUniprotToPdb,
					mapToUniprot.col("uniProtId").equalTo(mapUniprotToPdb.col("uniProtId"))
					.and(mapToUniprot.col("canonicalPosStart").equalTo(mapUniprotToPdb.col("uniProtPos"))))
					.drop(mapUniprotToPdb.col("insCode"))
					.drop(mapUniprotToPdb.col("uniProtId"))
					.drop(mapUniprotToPdb.col("uniProtPos"))
					.withColumnRenamed("pdbAtomPos","pdbPosStart");

			Dataset<Row> mapToPDBEnd = mapToUniprot.join(mapUniprotToPdb,
					mapToUniprot.col("uniProtId").equalTo(mapUniprotToPdb.col("uniProtId"))
					.and(mapToUniprot.col("canonicalPosEnd").equalTo(mapUniprotToPdb.col("uniProtPos"))))
					.drop(mapUniprotToPdb.col("insCode"))
					.drop(mapUniprotToPdb.col("uniProtId"))
					.drop(mapUniprotToPdb.col("uniProtPos"))
					.withColumnRenamed("pdbAtomPos","pdbPosEnd");

			Dataset<Row> mapToPDB = mapToPDBStart.join(mapToPDBEnd,
					mapToPDBStart.col("ensemblId").equalTo(mapToPDBEnd.col("ensemblId"))
							.and(mapToPDBStart.col("start").equalTo(mapToPDBEnd.col("start")))
							.and(mapToPDBStart.col("end").equalTo(mapToPDBEnd.col("end")))
							.and(mapToPDBStart.col("isoformIndex").equalTo(mapToPDBEnd.col("isoformIndex")))
							.and(mapToPDBStart.col("pdbId").equalTo(mapToPDBEnd.col("pdbId")))
							.and(mapToPDBStart.col("chainId").equalTo(mapToPDBEnd.col("chainId"))))
					.drop(mapToPDBEnd.col("chromosome")).drop(mapToPDBEnd.col("geneName"))
					.drop(mapToPDBEnd.col("ensemblId")).drop(mapToPDBEnd.col("geneBankId"))
					.drop(mapToPDBEnd.col("start")).drop(mapToPDBEnd.col("end"))
					.drop(mapToPDBEnd.col("orientation")).drop(mapToPDBEnd.col("offset"))
					.drop(mapToPDBEnd.col("uniProtId")).drop(mapToPDBEnd.col("canonicalPosStart"))
					.drop(mapToPDBEnd.col("canonicalPosEnd")).drop(mapToPDBEnd.col("isoformIndex"))
					.drop(mapToPDBEnd.col("isoformPosStart")).drop(mapToPDBEnd.col("isoformPosEnd"))
					.drop(mapToPDBEnd.col("pdbId")).drop(mapToPDBEnd.col("chainId"))
					.select("chromosome", "geneName", "ensemblId", "geneBankId", "start", "end", "orientation", "offset",
							"uniProtId", "canonicalPosStart", "canonicalPosEnd", "isoformIndex", "isoformPosStart", "isoformPosEnd",
							"pdbId","chainId","pdbPosStart","pdbPosEnd")
					.orderBy("start", "isoformIndex");

			mapToPDB.write().mode(SaveMode.Overwrite).parquet(pdbmapping+"/"+chr);
		}
	}

	public static void mapToHomologyModels(String uniprotmapping, String homologymodels, String mapping ) {

		Dataset<Row> models = SaprkUtils.getSparkSession().read().parquet(homologymodels);
		models.sort(models.col("template")).show();

//		String[] chromosomes = {"chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",
//				"chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19",  "chr20", "chr21", "chr22", "chrX", "chrY"};
		String[] chromosomes = {"chr21"};

		for (String chr : chromosomes) {

			Dataset<Row> mapToUniprot = SaprkUtils.getSparkSession().read().parquet(uniprotmapping+"/"+chr);
			mapToUniprot.show();

			Dataset<Row> mapToHomology = mapToUniprot.join(models,
					mapToUniprot.col("uniProtId").equalTo(models.col("uniProtId"))
						.and(
							mapToUniprot.col("canonicalPosStart").geq(models.col("fromUniprot"))
							.and(mapToUniprot.col("canonicalPosEnd").leq(models.col("toUniprot")))
							.and(mapToUniprot.col("orientation").equalTo("+"))
								.or(mapToUniprot.col("canonicalPosStart").geq(models.col("toUniprot"))
									.and(mapToUniprot.col("canonicalPosEnd").leq(models.col("fromUniprot")))
									.and(mapToUniprot.col("orientation").equalTo("-")))
						)
					)
					.drop(models.col("uniProtId"));
			mapToHomology.show();
		}
	}


	public static Dataset<Row> getGeneBankToEnsembleMapping() {

		// Ensembl to gene bank id mapping
		Dataset<Row> mp = SaprkUtils.getSparkSession()
				.read().csv(DataLocationProvider.getExonsProject()+"MAPS/mart_export.txt")
				.filter(t->t.getAs(1)!=null)
				.withColumnRenamed("_c0", "ensemblId")
				.withColumnRenamed("_c1", "geneBankId");
		return mp;
	}

	public static void mapExonsToGeneBank(List<ExonSerializable> exons, String path) throws IOException {

		Dataset<Row> exonsDF = SaprkUtils.getSparkSession().createDataFrame(exons, ExonSerializable.class);
		exonsDF.createOrReplaceTempView("exons");

		Dataset<Row> geneBankMapping = getGeneBankToEnsembleMapping();
		geneBankMapping.createOrReplaceTempView("genebank");

		Dataset<Row> mappingDF = SaprkUtils.getSparkSession().sql("select exons.chromosome, exons.geneName, exons.ensemblId, genebank.geneBankId, "
				+ "exons.orientation, exons.offset, exons.start, exons.end from exons inner join genebank on (exons.ensemblId = genebank.ensemblId)");
		mappingDF.write().mode(SaveMode.Overwrite).parquet(path);
	}

	public static void runGeneBankMapping() throws Exception {

		String datapath = path+"EXONS_DATA/gencode.v24.CDS.protein_coding.gtf";
		List<ExonSerializable> exons = ExonsUtils.getExonsData(datapath);

		String mappingpath = path+"MAPS/gencode.v24.CDS.protein_coding.gene_bank_mapping";
		mapExonsToGeneBank(exons, mappingpath);
	}



	public static void test() {

		String exonsuniprotpath = path+"MAPS/gencode.v24.CDS.protein_coding.uniprot_mapping/chr14";

		Encoder<ProteinFeatures> encoder = Encoders.bean(ProteinFeatures.class);
		Dataset<Row> data = SaprkUtils.getSparkSession().read().parquet(exonsuniprotpath);

		Dataset<ProteinFeatures> featuresDF = data.map(new MapToProteinDisorder(), encoder)
				.filter(t->t!=null);
		featuresDF.count();
	}

	public static void main(String[] args) throws Exception {

		long start = System.nanoTime();

		String homologymodels = "/Users/yana/data/genevariation/parquet/homology-models-mapping-pc30";

		String datapath = path+"MAPS/gencode.v24.CDS.protein_coding.gene_bank_mapping";
		String uniprotpath = path+"MAPS/gencode.v24.CDS.protein_coding.uniprot_mapping";
		String pdbpath = path+"MAPS/gencode.v24.CDS.protein_coding.pdb_mapping";
		String homologypath = path+"MAPS/gencode.v24.CDS.protein_coding.homology_mapping";

//		mapExonsToIsoformPositions(datapath, datapath);
//		mapToPDBPositions(uniprotpath, pdbpath);

		mapToHomologyModels( uniprotpath, homologymodels, homologypath );

//		Dataset<Row> map = SaprkUtils.getSparkSession().read().parquet(pdbpath+"/chr21");
//		map.show();

		System.out.println("Done: " + (System.nanoTime() - start) / 1E9 + " sec.");
	}
}
