package exonscorrelation;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.rcsb.genevariation.datastructures.ExonSerializable;
import org.rcsb.genevariation.utils.SaprkUtils;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class RemoveRedundancy {

	public static void newData() throws IOException {

		String path = "/Users/yana/ishaan/EXONS_DATA/";
		String datapath = "coordinated_exons.gencod.v24.csv";
		Dataset<Row> data = SaprkUtils.getSparkSession().read().csv(path+datapath);
		data.createOrReplaceTempView("data");
		data.show();

		String olddatapath = "FDR0.gene.CDS";
		Dataset<Row> olddata = SaprkUtils.getSparkSession().read().csv(path+olddatapath);
		olddata.createOrReplaceTempView("old");
		olddata.show();

		Dataset<Row> newexons = SaprkUtils.getSparkSession().sql("select data._c0, data._c1, data._c2, data._c3," +
				"data._c4, data._c5, data._c6 from data left join old on (old._c0=data._c0 and old._c1=data._c1 and old._c2=data._c2) where old._c0 is null");
		newexons.repartition(1).write().mode(SaveMode.Overwrite).csv(path+"FDR0.gene.CDS.new");

	}

	public static void groupData() throws IOException {

		String path = "/Users/yana/ishaan/EXONS_DATA/";
		String newdatapath = "coordinated_exons.gencod.v24.csv";
		List<String> lines = FileUtils.readLines(new File(path+newdatapath), "utf-8");

		Map<String,String[]> map = new HashMap<String,String[]>();

		for (String line : lines) {

			String[] annotationdata = line.split(",");

			String chromosome = annotationdata[0];
			String start = annotationdata[1];
			String end = annotationdata[2];
			String orientation = annotationdata[3];
			String phase = annotationdata[4];
			String ensemblId = annotationdata[5];
			String geneName = annotationdata[6];

			String key = String.join(",", Arrays.asList(chromosome,start,end,orientation,geneName));
			String[] value = new String[2];
			if (! map.containsKey(key)) {
				value[0] = ensemblId;
				value[1] = phase;
			}
			else {
				value = map.get(key);
				value[0] += ";"+ensemblId;
				value[1] += ";"+phase;
			}
			map.put(key, value);
		}

		PrintWriter writer = new PrintWriter(path+"coordinated_exons.new.gencod.v24.csv", "UTF-8");
		for (Map.Entry<String, String[]> m : map.entrySet()) {
			String line = m.getKey() + "," + m.getValue()[0] + ","  +m.getValue()[1];
			writer.println(line);
		}
	}

	public static void addData() throws IOException {

		String path = "/Users/yana/ishaan/EXONS_DATA/";
		String annotationpath = "gencode.v24.CDS.protein_coding.gtf";
		List<String> annotation = FileUtils.readLines(new File(path+annotationpath), "utf-8");

		String olddatapath = "FDR0.gene.CDS";
		Dataset<Row> olddata = SaprkUtils.getSparkSession().read().csv(path+olddatapath);
		olddata.createOrReplaceTempView("old");

		String newdatapath = "coordinated_exons.txt";
		List<String> lines = FileUtils.readLines(new File(path+newdatapath), "utf-8");

		List<ExonSerializable> exons = new ArrayList<ExonSerializable>();

		for (String annotationline : annotation) {

			String[] annotationdata = annotationline.split(",");

			String chromosome = annotationdata[0];
			String start = annotationdata[1];
			String end = annotationdata[2];
			String orientation = annotationdata[3];
			String phase = annotationdata[4];
			String ensemblId = annotationdata[5];
			String geneName = annotationdata[6];

			for (String line: lines) {

				String[] dat = line.split(",");

				String chromosomeExon = dat[0];
				String startExon = dat[1];
				String endExon = dat[2];
				String orientationExon = dat[3];

				if ( chromosome.equals(chromosomeExon) && start.equals(startExon) && end.equals(endExon) && orientation.equals(orientationExon) ) {

					ExonSerializable exon = new ExonSerializable();

					exon.setChromosome(chromosome);
					exon.setGeneName(geneName);
					exon.setEnsemblId(ensemblId);
					exon.setStart(Integer.valueOf(start));
					exon.setEnd(Integer.valueOf(end));
					exon.setOrientation(orientation);
					exon.setOffset(Integer.valueOf(phase));

					exons.add(exon);
				}
			}
		}

		Dataset<Row> exonsdata = SaprkUtils.getSparkSession().createDataFrame(exons, ExonSerializable.class);
		exonsdata.createOrReplaceTempView("new");

		Dataset<Row> newexons = SaprkUtils.getSparkSession().sql("select * from new where not exist (select _c0, _c1, _c2 from old where old._c0=new.chromosome and old._c1=new.start and old._c2=new.end)");
		newexons.show();
	}

	public static void removeRedundancy() throws IOException {

		int i = 0;

		List<String> linesNonRedundant = new ArrayList<String>();

		String uri = "/Users/yana/ishaan/EXONS_DATA/NEW_EXONS/FDR0.gene.CDS";
		List<String> lines = Files.readAllLines(Paths.get(uri));

		for (String line : lines) {

			String[] fields = line.split(",");

			String[] transcripts = new String[0];
			String[] phases = new String[0];
			try {
				transcripts = fields[5].split(";");
				phases = fields[6].split(";");
			} catch (Exception e) {
				e.printStackTrace();
			}

			Set<String> set = new HashSet<String>();
			Collections.addAll(set, phases);

			if (set.size()>1)
				i++;

			List<String> transcriptsNonRedundant = new ArrayList<String>();
			List<String> phasesNonRedundant = new ArrayList<String>();

			Iterator<String> it = set.iterator();
			while (it.hasNext()) {
				String phase = it.next();
				int index = Arrays.asList(phases).indexOf(phase);
				String transcript = transcripts[index];
				transcriptsNonRedundant.add(transcript);
				phasesNonRedundant.add(phase);
			}

			String newLine = fields[0]+","+fields[1]+","+fields[2]+","+fields[3]+","+fields[4]+","+String.join(";",transcriptsNonRedundant)+","+String.join(";",phasesNonRedundant);
			linesNonRedundant.add(newLine);
		}

		Path out = Paths.get("/Users/yana/ishaan/EXONS_DATA/NEW_EXONS/FDR0.gene.CDS.non_redundant");
		Files.write(out,linesNonRedundant,Charset.defaultCharset());
		System.out.println(i);
	}

	public static void main(String[] args) throws IOException {

		removeRedundancy();
	}
}
