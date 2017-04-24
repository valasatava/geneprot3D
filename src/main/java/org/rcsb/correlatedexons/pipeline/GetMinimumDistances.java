package org.rcsb.correlatedexons.pipeline;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.biojava.nbio.structure.Atom;
import org.biojava.nbio.structure.Chain;
import org.biojava.nbio.structure.Group;
import org.biojava.nbio.structure.Structure;
import org.rcsb.correlatedexons.mappers.MapToBestStructure;
import org.rcsb.correlatedexons.mappers.MapToResolution;
import org.rcsb.correlatedexons.utils.RowUtils;
import org.rcsb.correlatedexons.utils.StructureUtils;
import org.rcsb.genevariation.io.DataLocationProvider;
import org.rcsb.genevariation.utils.SaprkUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yana on 4/20/17.
 */
public class GetMinimumDistances {

    public static void run(String chr) throws IOException {

        Dataset<Row> mapping = SaprkUtils.getSparkSession()
                .read().parquet(DataLocationProvider.getExonsStructuralMappingLocation() + "/" + chr);

        JavaRDD<List<String>> test = mapping.toJavaRDD()
                .map(new MapToResolution())
                .groupBy(t -> (t.getString(2) + "_" + t.getString(3)))
                //.filter(t -> t._1.equals("ENST00000330714_NM_002463"))
                .map(new MapToBestStructure()).filter(t -> (t != null))

                .map(new Function<List<Row>, List<String>>() {
                    @Override
                    public List<String> call(List<Row> transcript) throws Exception {

                        List<String> results = new ArrayList<>();

                        try {

                            String gene = transcript.get(0).getString(0)+","+
                                          transcript.get(0).getString(1)+","+
                                          transcript.get(0).getString(2)+","+
                                          transcript.get(0).getString(3);

                            boolean pdbFlag = true;
                            if ( !RowUtils.isPDBStructure(transcript.get(0))) {
                                pdbFlag = false;
                            }

                            Chain chain;

                            if ( pdbFlag ) {
                                Structure structure = StructureUtils.getBioJavaStructure(RowUtils.getPdbId(transcript.get(0)));
                                chain = structure.getChain(RowUtils.getChainId(transcript.get(0)));
                            } else {
                                Structure structure = StructureUtils.getModelStructure(RowUtils.getModelCoordinates(transcript.get(0)));
                                chain = structure.getChainByIndex(0);
                            }

                            List<Group> groups = chain.getAtomGroups();

                            int numExons = transcript.size();
                            for (int i=0; i<numExons-1; i++) {
                                for (int j=i+1; j<numExons; j++) {

                                    Row exon1 = transcript.get(i);
                                    Row exon2 = transcript.get(j);

                                    if (  RowUtils.getExon(exon1).equals("204978968_204979069_2") && RowUtils.getExon(exon2).equals("204970616_204970747_2"))
                                        System.out.println();

                                    int start1=-1; int end1=-1;
                                    int start2=-1; int end2=-1;

                                    if ( RowUtils.isPDBStructure(exon1)) {

                                        start1 = RowUtils.getPdbStart(exon1);
                                        end1 = RowUtils.getPdbEnd(exon1);

                                        start2 = RowUtils.getPdbStart(exon2);
                                        end2 = RowUtils.getPdbEnd(exon2);
                                    }
                                    else {

                                        //UniprotToModelCoordinatesMapper mapper = new UniprotToModelCoordinatesMapper();
                                    }

                                    List<Atom> atoms1 = StructureUtils.getAtomsInRange(groups, start1, end1);
                                    List<Atom> atoms2 = StructureUtils.getAtomsInRange(groups, start2, end2);

                                    double mindist = StructureUtils.getMinDistance(atoms1, atoms2);
                                    String line = gene +","+RowUtils.getExon(exon1)+","+RowUtils.getExon(exon2)+","+String.valueOf(mindist)+"\n";
                                    results.add(line);
                                }
                            }

                        } catch ( Exception e){
                            e.printStackTrace();
                        }
                        return results;
                    }
                });

        List<List<String>> results = test.collect();
        List<String> out = new ArrayList<>();

        for (List<String> t : results) {
            for (String l : t ) {
                out.add(l);
            }
        }

        String path = "/Users/yana/ishaan/RESULTS/distances/";

        File file = new File(path+chr);
        if (!file.exists()) {
            if (file.mkdir()) {
                System.out.println("Directory is created for "+chr);
            } else {
                System.out.println("Directory for "+chr+" already exists!");
            }
        }

        String filename = path+chr+"/minimum_distances.csv";
        FileWriter writer = new FileWriter(filename);
        for(String str: out) {
            writer.write(str);
        }
        writer.close();
    }

    public static void main(String[] args) throws IOException {
        run("chr21");
    }
}