package org.rcsb.geneprot.transcriptomics.utils;

import com.google.common.collect.Range;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.rcsb.geneprot.common.mappers.UniprotToModelCoordinatesMapper;
import org.rcsb.uniprot.auto.FeatureType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import static java.lang.Math.abs;

/**
 * Created by yana on 4/20/17.
 */
public class RowUtils {

    private static int chromosomeInd = 0;
    private static int geneNameInd = 1;
    private static int ensemblIdInd = 2;
    private static int geneBankIdInd = 3;

    private static int alignmentInd = 17;

    public static String getChromosome(Row row) {
        return row.getString(chromosomeInd);
    }
    public static String getGeneName(Row row) {
        return row.getString(geneNameInd);
    }
    public static String getEnsemblId(Row row) {
        return row.getString(ensemblIdInd);
    }
    public static String getGeneBankId(Row row) {
        return row.getString(geneBankIdInd);
    }

    public static String getPdbId(Row row) {
        String pdbId = row.getString(11);
        if (pdbId.equals("null")) {
            pdbId = row.getString(15).split(Pattern.quote("."))[0];
        }
        return pdbId;
    }
    public static String getChainId(Row row) {

        String chainId = row.getString(12);
        if (chainId.equals("null")) {
            chainId = row.getString(15).split(Pattern.quote("."))[2];
        }
        return chainId;
    }

    public static float getResolution(Row row) {

        if (row.get(17) != null) {
            return row.getFloat(17);
        } else {
            return 999.9f;
        }
    }

    public static String getExon(Row row) {
        return String.valueOf(row.getInt(4)) + "_" +
                String.valueOf(row.getInt(5)) + "_" +
                String.valueOf(row.getInt(7));
    }

    public static boolean isPDBStructure(Row row) {
        if (row.getString(11).equals("null"))
            return false;
        return true;
    }

    public static boolean isForward(Row row) {
        if (row.getString(6).equals("+"))
            return true;
        return false;
    }

    public static String getModelCoordinates(Row row) {
        return row.getString(16);
    }

    public static int getUniProtStart(Row row) {

        if (isForward(row)) {

            if (row.get(9) == null) {
                return -1;
            } else {
                return row.getInt(9);
            }
        } else {
            if (row.get(10) == null) {
                return -1;
            } else {
                return row.getInt(10);
            }
        }
    }

    public static int getUniProtEnd(Row row) {

        if (isForward(row)) {
            if (row.get(10) == null) {
                return -1;
            } else {
                return row.getInt(10);
            }
        } else {
            if (row.get(9) == null) {
                return -1;
            } else {
                return row.getInt(9);
            }
        }
    }

    public static Range<Integer> getModelRange(UniprotToModelCoordinatesMapper mapper, Row row) {

        int uniStart = getUniProtStart(row);
        int uniEnd = getUniProtEnd(row);

        int start = -1; int end = -1;

        int modelFrom = getModelFrom(row);
        if ( modelFrom > uniStart ) {
            start = mapper.getModelCoordinateByUniprotPosition(modelFrom);
        }
        else {
            start = mapper.getModelCoordinateByUniprotPosition(uniStart);
        }

        int modelTo = getModelTo(row);
        if ( modelTo < uniEnd )
            end = mapper.getModelCoordinateByUniprotPosition(modelTo);
        else {
            end = mapper.getModelCoordinateByUniprotPosition(uniEnd);
        }

        if ( start == -1 ) {
            start = Integer.MIN_VALUE;
            int resModelStart = mapper.getFirstResidueNumber();
            if ((resModelStart > uniStart) && (resModelStart < uniEnd) ) {
                start = resModelStart;
            }
        }

        if ( end == -1 ) {
            end = Integer.MAX_VALUE;
            int resModelEnd = mapper.getLastResidueNumber();
            if ( (resModelEnd > uniStart) && (resModelEnd < uniEnd) ) {
                end = resModelEnd;
            }
        }
        return Range.closed(start, end);
    }

    public static Range<Integer> getPdbRange(Row row) {

        int start = getPdbStart(row);
        int end = getPdbEnd(row);

        if ( start == -1 ) {
            start = Integer.MIN_VALUE;
        }
        if ( end == -1 ) {
            end = Integer.MAX_VALUE;
        }

        if (end > start) {
            return Range.closed(start, end);
        }
        else {
            return Range.closed(end, start);
        }
    }

    public static int getPdbStart(Row row) {

        if (isForward(row)) {
            if (row.get(13) == null)
                return -1;
            return row.getInt(13);
        } else {
            if (row.get(14) == null)
                return -1;
            return row.getInt(14);
        }
    }

    public static int getPdbEnd(Row row) {

        if (isForward(row)) {
            if (row.get(14) == null)
                return -1;
            return row.getInt(14);
        } else {
            if (row.get(13) == null)
                return -1;
            return row.getInt(13);
        }
    }

    public static int getModelFrom(Row row) {

        if (row.get(13) == null)
            return -1;
        return row.getInt(13);
    }

    public static int getModelTo(Row row) {

        if (row.get(14) == null)
            return -1;
        return row.getInt(14);
    }

    public static String getUniProtId(Row row) {
        return row.getString(8);
    }

    public static String getTemplate(Row row) {
        return row.getString(15);
    }

    public static String getCoordinates(Row row) {
        return row.getString(16);
    }

    public static Row addField(Row row, Object field) {

        Object[] fields = new Object[row.length()+1];
        for (int c=0; c < row.length(); c++)
            fields[c] = row.get(c);
        fields[row.length()] = field;

        return RowFactory.create(fields);

    }

    public static void setUTMmapperFromRow(UniprotToModelCoordinatesMapper mapper, Row row) throws Exception {

        mapper.setFrom(RowUtils.getModelFrom(row));
        mapper.setTo(RowUtils.getModelTo(row));
        mapper.setAlignment(RowUtils.getAlignment(row));
        mapper.setTemplate(RowUtils.getTemplate(row));

        mapper.map();

    }

    public static int getStructureStart(Row row) throws Exception {

        int residueNumber;

        if (isPDBStructure(row)) {
            residueNumber = getPdbStart(row);
            if ( residueNumber == -1 ) {
                //TODO: get the residue mapping for the head
            }
        }
        else {
            UniprotToModelCoordinatesMapper mapper = new UniprotToModelCoordinatesMapper();
            setUTMmapperFromRow(mapper, row);
            Range<Integer> range = RowUtils.getModelRange(mapper, row);
            residueNumber = range.lowerEndpoint();
        }
        return residueNumber;
    }

    public static int getStructureEnd(Row row) throws Exception {

        int residueNumber;

        if (isPDBStructure(row)) {
            residueNumber = getPdbEnd(row);
            if ( residueNumber == -1 ) {
                //TODO: get the residue mapping for the tail
            }
        }
        else {
            UniprotToModelCoordinatesMapper mapper = new UniprotToModelCoordinatesMapper();
            setUTMmapperFromRow(mapper, row);
            Range<Integer> range = RowUtils.getModelRange(mapper, row);
            residueNumber = range.upperEndpoint();
        }
        return residueNumber;
    }

    public static String getAlignment(Row row) {
        return row.getString(alignmentInd);
    }

    public static Row mapToFeatureRange(FeatureType ft, Range<Integer> coveredRange, Row row) {

        row = RowUtils.addField(row, ft.getDescription());
        row = RowUtils.addField(row, coveredRange.lowerEndpoint());
        row = RowUtils.addField(row, coveredRange.upperEndpoint());
        return row;
    }

    public static Row mapToFeatureResidue(FeatureType ft, int i, Row row) {
        row = RowUtils.addField(row, ft.getDescription());
        row = RowUtils.addField(row, i);
        return row;
    }

    public static List<Row> getPDBStructure(Iterable<Row> data, String[] key) {

        String pdbId = key[0];
        String chainId = key[1];

        Iterator<Row> it = data.iterator();

        List<Row> bestStruc = new ArrayList<Row>();
        while (it.hasNext()) {
            Row row = it.next();
            if (row.getString(11).equals(pdbId) && row.getString(12).equals(chainId)) {
                bestStruc.add(row);
            }
        }
        return bestStruc;
    }

    public static List<Row> getModelStructure(Iterable<Row> data, String[] key) {

        String pdbId = key[0];
        String chainId = key[1];

        int start = Integer.valueOf(key[2]);
        int end = Integer.valueOf(key[3]);

        Iterator<Row> it = data.iterator();
        List<Row> best = new ArrayList<Row>();

        while (it.hasNext()) {
            Row row = it.next();
            if (row.getString(15).contains(pdbId) && row.getString(15).contains(chainId)
                    && row.getInt(13)==start && row.getInt(14)==end) {
                best.add(row);
            }
        }
        return best;
    }

    public static String[] getStructureWithBestResolution(Iterable<Row> data, List<String> keys) throws Exception {

        String[] best = new String[4];

        if ( keys.size() == 1 ) {
            best[0] = keys.get(0).split("_")[0];
            best[1] = keys.get(0).split("_")[1];
            best[2] = keys.get(0).split("_")[3];
            best[3] = keys.get(0).split("_")[4];
        }
        else {

            int bestStart = -1;
            int bestEnd = -1;
            float bestRes = 99.9f;

            Iterator<Row> it = data.iterator();

            for (String key : keys) {

                String pdbIdb = key.split("_")[0];
                String chainId = key.split("_")[1];
                int start = Integer.valueOf(key.split("_")[3]);
                int end = Integer.valueOf(key.split("_")[4]);

                while (it.hasNext()) {

                    Row row = it.next();

                    String pdbIdRow = getPdbId(row);
                    String chainIdRow = getChainId(row);

                    int startRow = getStructureStart(row);
                    int endRow = getStructureEnd(row);

                    if ( pdbIdb.equals(pdbIdRow) && chainId.equals(chainIdRow) && start==startRow && end==endRow ) {

                        float resolution = getResolution(row);

                        if (resolution < bestRes) {

                            best[0] = pdbIdRow;
                            best[1] = chainIdRow;
                            best[2] = String.valueOf(startRow);
                            best[3] = String.valueOf(endRow);

                            bestRes = resolution;
                            bestStart = startRow;
                            bestEnd = endRow;
                        }
                        else if (resolution == bestRes) {

                            if ( (bestStart != -1 && start != -1) && (bestEnd != -1 && end != -1) ) {

                                if ( abs(end-start) > abs(bestEnd-bestStart) ) {

                                    best[0] = pdbIdRow;
                                    best[1] = chainIdRow;
                                    best[2] = String.valueOf(bestStart);
                                    best[3] = String.valueOf(bestEnd);

                                    bestStart = start;
                                    bestEnd = end;

                                }
                            }
                        }
                    }
                }
            }
        }
        return best;
    }

    public static List<Integer> getIsoStartEndForRow(Row row) {

        int isoformStart;
        int isoformEnd;

        String orientation = row.getString(5);
        if (orientation.equals("+")) {
            isoformStart = row.getInt(0);
            isoformEnd = row.getInt(11);
        } else {
            isoformStart = row.getInt(11);
            isoformEnd = row.getInt(0);
        }
        return Arrays.asList(isoformStart, isoformEnd);
    }
}