package exonscorrelation;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;

public class MapToExonData implements  FlatMapFunction<Row, ExonData> {

	private static final long serialVersionUID = -1871761147320947320L;

	@Override
	public Iterator<ExonData> call(Row row) throws Exception {
		
		List<ExonData> data = new ArrayList<ExonData>();
		
		String chromosome = row.getString(0);
		String orientation = row.getString(3);
		String geneName = row.getString(4);
		Integer start = Integer.valueOf(row.getString(1));
		Integer end = Integer.valueOf(row.getString(2));
		
		String[] ensemblIds = row.getString(5).split(";");
		String[] offsets = row.getString(6).split(";");
		
		for (int i=0; i< ensemblIds.length; i++) {
			ExonData exon = new ExonData(chromosome, geneName, start, end, orientation);
			try {
				exon.setEnsemblId(ensemblIds[i].split(Pattern.quote("."))[0]);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			exon.setOffset(Integer.valueOf(offsets[i]));
			data.add(exon);
		}
		return data.iterator();
	}
}
