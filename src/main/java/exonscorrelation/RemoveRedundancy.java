package exonscorrelation;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class RemoveRedundancy {
	
	public static void main(String[] args) throws IOException {
		
		int i = 0;
		
		List<String> linesNonRedundant = new ArrayList<String>();
		
		String uri = "/Users/yana/ishaan/FDR0.gene.CDS";
		List<String> lines = Files.readAllLines(Paths.get(uri));
		
		for (String line : lines) {
			
			String[] fields = line.split(",");
			
			String[] transcripts = fields[5].split(";");
			String[] phases = fields[6].split(";");
			
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
		
		Path out = Paths.get("/Users/yana/ishaan/FDR0.gene.CDS.non_redundant");
		Files.write(out,linesNonRedundant,Charset.defaultCharset());
		System.out.println(i);
	}
}
