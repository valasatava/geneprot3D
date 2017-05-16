package org.rcsb.exonscoassociation.utils;

import com.google.common.collect.Range;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.rcsb.genevariation.mappers.UniprotToModelCoordinatesMapper;

import java.util.regex.Pattern;

/**
 * Created by yana on 4/20/17.
 */
public class RowUtils {

    private static int alignmentInd = 17;

    public static String getEnsemblId(Row row) {
        return row.getString(2);
    }

    public static String getGeneBankId(Row row) {
        return row.getString(3);
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
}