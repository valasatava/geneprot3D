package org.rcsb.correlatedexons;

import org.rcsb.correlatedexons.mappers.MapToProteinDisorder;
import org.rcsb.correlatedexons.pipeline.DRunHomologyModelsMapping;
import org.apache.spark.sql.*;
import org.rcsb.genevariation.datastructures.ProteinFeatures;
import org.rcsb.genevariation.io.DataLocationProvider;
import org.rcsb.genevariation.utils.SaprkUtils;

public class RunExonsPipeline {

	public static void test() {

		String exonsuniprotpath = DataLocationProvider.getExonsProject()
				+"MAPS/gencode.v24.CDS.protein_coding.uniprot_mapping/chr14";

		Encoder<ProteinFeatures> encoder = Encoders.bean(ProteinFeatures.class);
		Dataset<Row> data = SaprkUtils.getSparkSession().read().parquet(exonsuniprotpath);

		Dataset<ProteinFeatures> featuresDF = data.map(new MapToProteinDisorder(), encoder)
				.filter(t->t!=null);
		featuresDF.count();
	}


	public static void main(String[] args) throws Exception {

		long start = System.nanoTime();

//		ARunGeneBankMapping.runGencodeV24();
//		BRunUniprotMapping.runGencodeV24();
//		CRunPDBStructuresMapping.runGencodeV24();
		DRunHomologyModelsMapping.runGencodeV24();

		System.out.println("Done: " + (System.nanoTime() - start) / 1E9 + " sec.");
	}
}
