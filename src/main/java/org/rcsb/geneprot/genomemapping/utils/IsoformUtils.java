package org.rcsb.geneprot.genomemapping.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.rcsb.geneprot.common.utils.CommonConstants;
import org.rcsb.geneprot.common.utils.ExternalDBUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Yana Valasatava on 10/23/17.
 */
public class IsoformUtils {

    private static final Logger logger = LoggerFactory.getLogger(IsoformUtils.class);

    public static String insert(String isoform, String variation, int pos, IndexOffset offset) {

        short insertionLength = (short) variation.length();
        int offsetPos = offset.getOffset(pos - 1);

        String seqBeforeInsertion = isoform.substring(0, offsetPos);

        String seqAfterInsertion = "";
        if (pos < isoform.length())
            seqAfterInsertion = isoform.substring(offsetPos, isoform.length());

        String modified = seqBeforeInsertion + variation + seqAfterInsertion;

        offset.setOffset(pos, offset.getLength(), insertionLength);

        return modified;
    }

    public static String replace(String isoform, int begin, int end, String variation, IndexOffset offset) {

        int beginOffset = offset.getOffset(begin - 1);
        int endOffset = offset.getOffset(end - 1);

        // part of the sequence before variation
        String seqBeforeVar = "";
        if (begin > 1)
            seqBeforeVar = isoform.substring(0, beginOffset);

        // variation of the sequence
        String seqVar = variation;

        // part of the sequence after variation
        String seqAfterVar = "";
        if (isoform.length() - endOffset > 0)
            seqAfterVar = isoform.substring(endOffset + 1, isoform.length());

        // combine sequence parts including variant
        String modifiedSequence = seqBeforeVar + seqVar + seqAfterVar;

        // the length of the sequence being replaced
        short varLength = (short) (end - begin);
        short diff = (short) (seqVar.length() - varLength - 1);

        int oldOffsetLength = offset.getLength();
        if (modifiedSequence.length() > offset.getLength())
            offset.adjustOffset(modifiedSequence.length());

        // reset offset array to match a given modified sequence
        offset.setOffset(end, oldOffsetLength, diff);

        return modifiedSequence;
    }

    public static String delete(String isoform, int begin, int end, IndexOffset offset) {

        int beginOffset = offset.getOffset(begin - 1);
        int endOffset = offset.getOffset(end - 1);

        String seqBeforeDeletion = "";
        if (begin > 1)
            seqBeforeDeletion = isoform.substring(0, beginOffset);

        // end offset + 2 verified in Q92994-2 VSP_006396
        String seqAfterDeletion = "";
        if ((isoform.length() - endOffset) > 0)
            seqAfterDeletion = isoform.substring(endOffset + 1, isoform.length());
        // combine parts of the sequence excluding the deletion
        String modifiedSequence = seqBeforeDeletion + seqAfterDeletion;

        short delLength = (short) (end - begin + 1);
        short modifier = (short) (0 - delLength);

        // reset the offsets and resize the offsets array
        offset.setOffset(begin, offset.getLength(), modifier);

        return modifiedSequence;
    }

    public static String buildIsoform(String canonical, String features) {

        IndexOffset offset = new IndexOffset(canonical.length());

        String isoform = canonical;
        for (String featureId : features.split(" "))
        {
            Dataset<Row> range = ExternalDBUtils.getSequenceVariationInRange(featureId).cache();
            if (range.collectAsList().size() > 0) {

                Row v = range.first();

                int begin = new Long(v.getLong(v.schema().fieldIndex(CommonConstants.COL_BEGIN))).intValue();
                int end = new Long(v.getLong(v.schema().fieldIndex(CommonConstants.COL_END))).intValue();

                // replacement
                if (v.get(v.schema().fieldIndex(CommonConstants.COL_VARIATION)) != null) {
                    String variation = v.getString(v.schema().fieldIndex(CommonConstants.COL_VARIATION));
                    isoform = IsoformUtils.replace(isoform, begin, end, variation, offset);
                }
                else { // deletion
                    isoform = IsoformUtils.delete(isoform, begin, end, offset);
                }
            }
            else { // this feature modifies a single amino acid
                Row v = null;
                try {
                    v = ExternalDBUtils.getSequenceVariationAtPosition(featureId).first();
                } catch (Exception e) {
                    logger.error("Cannot retrieve variation for {}", featureId);
                }
                String original = v.getString(v.schema().fieldIndex(CommonConstants.COL_ORIGINAL));

                int pos = new Long(v.getLong(v.schema().fieldIndex(CommonConstants.COL_POSITION))).intValue();

                if ((original == null || original.length() == 0)
                        && v.get(v.schema().fieldIndex(CommonConstants.COL_VARIATION)) != null) {
                    String variation = v.getString(v.schema().fieldIndex(CommonConstants.COL_VARIATION));
                    isoform = insert(isoform, variation, pos, offset);
                }
                else if (v.get(v.schema().fieldIndex(CommonConstants.COL_VARIATION)) != null) {
                    String variation = v.getString(v.schema().fieldIndex(CommonConstants.COL_VARIATION));
                    isoform = replace(isoform, pos, pos, variation, offset);
                } else {
                    // delete a point mutation
                    isoform = delete(isoform, pos, pos, offset);
                }
            }
        }
        return isoform;
    }

    public static Map<String, String> buildIsoforms(String uniProtId) {

        List<Row> comments = ExternalDBUtils.getSequenceComments(uniProtId).collectAsList();

        if ( comments.size() == 0 )
            return null; // no alternative splicing events happen

        List<Row> seqQuery = ExternalDBUtils.getCanonicalUniProtSequence(uniProtId)
                .collectAsList();
        if ( seqQuery.size() == 0 ) {
            logger.error("Could not retrieve canonical sequence for {}", uniProtId);
            return null;
        }

        String canonical = seqQuery.get(0).getString(0);

        Map<String, String> isoforms = new HashMap<>();

        for (Row c : comments)
        {
            String features;
            try {
                features = c.getString(c.schema().fieldIndex(CommonConstants.COL_FEATURE_ID));
            } catch (Exception e) {
                logger.error("Could not retrieve features for {}", uniProtId);
                return null;
            }

            if (features == null) {
                String moleculeId = c.getString(c.schema().fieldIndex(CommonConstants.MOLECULE_ID));
                isoforms.put(moleculeId, canonical);
            }
            else {
                String isoform = buildIsoform(canonical, features);
                String moleculeId = c.getString(c.schema().fieldIndex(CommonConstants.MOLECULE_ID));
                isoforms.put(moleculeId, isoform);
            }
        }
        return isoforms;
    }

    public static void main(String[] args) {

        Map<String, String> isoforms = buildIsoforms("Q8NA29");
        System.out.println();
    }
}
