package org.rcsb.genevariation.utils;

import java.util.ArrayList;
import java.util.List;

public class CommonUtils {
	
	public static List<Integer> getIntegerListFromString(String lst, String sep){
		String[] spl = lst.split(sep);
		ArrayList<Integer> l = new ArrayList<Integer>();
		for (String s : spl){
			l.add(Integer.parseInt(s));
		}
		l.trimToSize();
		return l;
	}

}
