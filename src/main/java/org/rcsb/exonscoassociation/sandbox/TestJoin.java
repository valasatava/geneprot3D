package org.rcsb.exonscoassociation.sandbox;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.rcsb.genevariation.utils.SaprkUtils;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.not;


/**
 * Created by yana on 4/17/17.
 */
public class TestJoin {

    public static void main(String[] args) {

        List<BinOne> binsone = new ArrayList<BinOne>();
        binsone.add(new BinOne(0, 5));
        binsone.add(new BinOne(1, 7));
        binsone.add(new BinOne(2, 3));
        binsone.add(new BinOne(4, 15));

        Encoder<BinOne> boEncoder = Encoders.bean(BinOne.class);
        Dataset<BinOne> bo = SaprkUtils.getSparkSession().createDataset(binsone, boEncoder);
        //bo.show();

        List<BinTwo> binstwo = new ArrayList<BinTwo>();
        binstwo.add(new BinTwo(0, 10));
        binstwo.add(new BinTwo(2, 8));
        binstwo.add(new BinTwo(3, 10));
        binstwo.add(new BinTwo(4, 10));

        Encoder<BinTwo> btEncoder = Encoders.bean(BinTwo.class);
        Dataset<BinTwo> bt = SaprkUtils.getSparkSession().createDataset(binstwo, btEncoder);
        //bt.show();

        // doesn't keep duplicates
        List<String> columnnames = new ArrayList<String>(){{add("id");}};
        Seq<String> columns = JavaConversions.asScalaBuffer(columnnames).toSeq();
        Dataset<Row> outer = bo.join(bt, columns, "outer");
        //outer.show();

        // test reverse
        List<FromTo> fromtos = new ArrayList<FromTo>(){{add(new FromTo(2,5));add(new FromTo(7,13));add(new FromTo(8,18));
                                                        add(new FromTo(14,30));add(new FromTo(15,25));add(new FromTo(20,25));
        }};
        Encoder<FromTo> ftEncoder = Encoders.bean(FromTo.class);
        Dataset<FromTo> ft = SaprkUtils.getSparkSession().createDataset(fromtos, ftEncoder);
        ft.show();

        Dataset<Row> tbl = outer.filter(outer.col("id").equalTo(4));
        tbl.show();

        Dataset<Row> ranges = tbl.join(ft, not((ft.col("to").leq(tbl.col("end")).and(ft.col("from").leq(tbl.col("end"))))
                .or(ft.col("from").geq(tbl.col("start")).and(ft.col("to").geq(tbl.col("start"))))
        ), "left");
        ranges.show();
    }
}
