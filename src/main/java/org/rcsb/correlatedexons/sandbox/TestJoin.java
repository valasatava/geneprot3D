package org.rcsb.correlatedexons.sandbox;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.rcsb.genevariation.utils.SaprkUtils;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by yana on 4/17/17.
 */
public class TestJoin {

    public static void main(String[] args) {

        List<BinOne> binsone = new ArrayList<BinOne>();
        binsone.add(new BinOne(0, 5));
        binsone.add(new BinOne(1, 7));
        binsone.add(new BinOne(2, 3));

        Encoder<BinOne> boEncoder = Encoders.bean(BinOne.class);
        Dataset<BinOne> bo = SaprkUtils.getSparkSession().createDataset(binsone, boEncoder);
        bo.show();

        List<BinTwo> binstwo = new ArrayList<BinTwo>();
        binstwo.add(new BinTwo(0, 10));
        binstwo.add(new BinTwo(2, 8));
        binstwo.add(new BinTwo(3, 10));

        Encoder<BinTwo> btEncoder = Encoders.bean(BinTwo.class);
        Dataset<BinTwo> bt = SaprkUtils.getSparkSession().createDataset(binstwo, btEncoder);
        bt.show();

        // doesn't keep duplicates
        List<String> columnnames = new ArrayList<String>(){{add("id");}};
        Seq<String> columns = JavaConversions.asScalaBuffer(columnnames).toSeq();
        Dataset<Row> outer = bo.join(bt, columns, "outer");
        outer.show();

        // keeps duplicated columns
        Dataset<Row> inner = bo.join(bt, bo.col("id").equalTo(bt.col("id")));
        inner.show();

    }
}
