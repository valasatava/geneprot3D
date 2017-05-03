package org.rcsb.exonscoassociation.filters;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

/**
 * Created by yana on 4/19/17.
 */
public class FilterBounaries implements Function<Row, Row> {

    @Override
    public Row call(Row row) throws Exception {

        if ( ! row.getString(11).equals("null") ) {
            if ( (row.get(13)==null) || (row.get(14)==null) )
                return null;
        }
        else if ( ! row.getString(15).equals("null") ) {
            if (row.getString(6).equals("+")) {
                if ( (row.getInt(13)>row.getInt(9)) || (row.getInt(14)<row.getInt(10)) )
                    return null;
            }
            else {
                if ( (row.getInt(13)>row.getInt(10)) || (row.getInt(14)<row.getInt(9)) )
                    return null;
            }
        }
        return row;
    }
}
