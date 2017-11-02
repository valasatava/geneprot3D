package org.rcsb.geneprot.genomemapping.utils;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.collection.JavaConversions;

import java.util.Arrays;

/**
 * Created by Yana Valasatava on 10/30/17.
 */
public class RowUpdater {

    public static Row updateField(Row row, String fieldName, Object fieldValue) {

        String[] fieldsNames = row.schema().fieldNames();
        scala.collection.immutable.Map<String, Object> values = row
                .getValuesMap(JavaConversions.asScalaBuffer(Arrays.asList(fieldsNames)).toSeq());

        Object[] objArray = new Object[fieldsNames.length];
        for ( int i=0; i<fieldsNames.length; i++ ){

            if (fieldsNames[i].equals(fieldName)) {
                objArray[i] = fieldValue;
            } else {
                Option<Object> val = values.get(fieldsNames[i]);
                objArray[i]=val.get();
            }
        }
        return new GenericRowWithSchema(objArray, row.schema());
    }

    public static Row addField(Row row, String fieldName, Object fieldValue, DataType type) {

        String[] fieldsNames = row.schema().fieldNames();
        scala.collection.immutable.Map<String, Object> values = row
                .getValuesMap(JavaConversions.asScalaBuffer(Arrays.asList(fieldsNames)).toSeq());

        Object[] objArray = new Object[fieldsNames.length+1];
        for ( int i=0; i<fieldsNames.length; i++ ){
            Option<Object> val = values.get(fieldsNames[i]);
            objArray[i]=val.get();
        }
        objArray[fieldsNames.length] = fieldValue;

        StructType schema = row.schema();
        schema = schema.add(fieldName, type);

        return new GenericRowWithSchema(objArray, schema);
    }

    public static Row addArray(Row row, String fieldName, Object array, DataType type) {

        String[] fieldsNames = row.schema().fieldNames();
        scala.collection.immutable.Map<String, Object> values = row
                .getValuesMap(JavaConversions.asScalaBuffer(Arrays.asList(fieldsNames)).toSeq());

        Object[] objArray = new Object[fieldsNames.length+1];
        for ( int i=0; i<fieldsNames.length; i++ ){
            Option<Object> val = values.get(fieldsNames[i]);
            objArray[i]=val.get();
        }
        objArray[fieldsNames.length] = array;

        StructType schema = row.schema();
        schema = schema.add(fieldName, ArrayType.apply(type));

        return new GenericRowWithSchema(objArray, schema);
    }
}
