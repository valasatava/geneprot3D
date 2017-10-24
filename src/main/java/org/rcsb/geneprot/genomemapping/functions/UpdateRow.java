package org.rcsb.geneprot.genomemapping.functions;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import scala.Option;
import scala.Tuple3;
import scala.collection.JavaConversions;

import java.io.Serializable;
import java.util.Arrays;


/**
 * Created by Yana Valasatava on 10/24/17.
 */
public class UpdateRow implements Function<Tuple3<Row, String, String>, Row>, Serializable {

    @Override
    public Row call(Tuple3<Row, String, String> t) throws Exception {

        Row row = t._1();
        String[] fieldsNames = row.schema().fieldNames();
        scala.collection.immutable.Map<String, Object> values = row
                .getValuesMap(JavaConversions.asScalaBuffer(Arrays.asList(fieldsNames)).toSeq());

        Object[] objArray = new Object[fieldsNames.length];
        for ( int i=0; i<fieldsNames.length; i++ ){

            if (fieldsNames[i].equals(t._2())) {
                objArray[i] = t._3();
            } else {
                Option<Object> val = values.get(fieldsNames[i]);
                objArray[i]=val.get();
            }
        }
        return RowFactory.create(objArray);
    }
}