package org.rcsb.exonscoassociation.utils;

import com.google.common.collect.Range;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;
import org.rcsb.genevariation.io.DataLocationProvider;
import org.rcsb.genevariation.mappers.UniprotToModelCoordinatesMapper;
import org.rcsb.genevariation.utils.SaprkUtils;

import static org.apache.spark.sql.functions.col;
import static org.junit.Assert.assertEquals;

/**
 * Created by yana on 4/30/17.
 */
public class TestRowUtils {

    Dataset<Row> mapping;
    @Before
    public void setup() {
        mapping = SaprkUtils.getSparkSession().read()
                .parquet(DataLocationProvider.getGencodeStructuralMappingLocation() + "/chr21");
    }

    @Test
    public void testGetPdbRangesOnReverseStrand() {

        Dataset<Row> data = mapping.filter(col("pdbId").equalTo("2LP1")
                .and(col("start").equalTo(25891722)).and(col("end").equalTo(25891868)));
        Row row = data.collectAsList().get(0);

        Range<Integer> range = RowUtils.getPdbRange(row);

        assertEquals(689, range.lowerEndpoint().intValue());
        assertEquals(Integer.MAX_VALUE, range.upperEndpoint().intValue());
    }

    @Test
    public void testGetPdbRangesOnForwardStrand() {

        Dataset<Row> data = mapping.filter(col("pdbId").equalTo("2XY1")
                .and(col("start").equalTo(21373863)).and(col("end").equalTo(21374013)));

        Row row = data.collectAsList().get(0);

        Range<Integer> range = RowUtils.getPdbRange(row);

        assertEquals(349, range.lowerEndpoint().intValue());
        assertEquals(Integer.MAX_VALUE, range.upperEndpoint().intValue());
    }

    @Test
    public void testGetModelRangesOnReverseStrand() throws Exception {

        Dataset<Row> data = mapping.filter(col("template").equalTo("4v1a.1.C")
                .and(col("start").equalTo(25585710)).and(col("end").equalTo(25585754)));
        Row row = data.collectAsList().get(0);

        UniprotToModelCoordinatesMapper mapper = new UniprotToModelCoordinatesMapper(RowUtils.getUniProtId(row));
        String url = RowUtils.getCoordinates(row);
        mapper.setTemplate(url);
        mapper.map();

        Range<Integer> range = RowUtils.getModelRange(mapper, row);

        assertEquals(324, range.lowerEndpoint().intValue());
        assertEquals(328, range.upperEndpoint().intValue());
    }

    @Test
    public void testGetModelRangesOnForwardStrand() throws Exception {

        Dataset<Row> data = mapping.filter(col("template").equalTo("4ofy.1.B")
                .and(col("start").equalTo(21286269)).and(col("end").equalTo(21286412)));
        Row row = data.collectAsList().get(0);

        UniprotToModelCoordinatesMapper mapper = new UniprotToModelCoordinatesMapper(RowUtils.getUniProtId(row));
        String url = RowUtils.getCoordinates(row);
        mapper.setTemplate(url);
        mapper.map();

        Range<Integer> range = RowUtils.getModelRange(mapper, row);

        assertEquals(116, range.lowerEndpoint().intValue());
        assertEquals(161, range.upperEndpoint().intValue());
    }
}
