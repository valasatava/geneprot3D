package org.rcsb.geneprot.genomemapping.utils;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF2;
import scala.collection.mutable.WrappedArray;

/**
 * Created by Yana Valasatava on 12/11/17.
 */
public class UDFs {

    /**
     *
     */
    public static UDF2 merge_coordinates_mapping = new UDF2<WrappedArray<Row>, WrappedArray<Row>, WrappedArray<Row>>() {

        @Override
        public WrappedArray<Row> call(WrappedArray<Row> coordinatesMappingGenomic, WrappedArray<Row> coordinatesMappingEntity) throws Exception {

            return null;
        }
    };
}
