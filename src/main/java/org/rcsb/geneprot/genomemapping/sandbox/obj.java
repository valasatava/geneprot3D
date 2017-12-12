package org.rcsb.geneprot.genomemapping.sandbox;

/**
 * Created by Yana Valasatava on 12/1/17.
 */
public class obj {


        private Object Object1 = null;
        private Object Object2 = null;

        public obj(){
            this.Object1 = new o1();
            this.Object2 = new o2();
        }

        // You can provide setters to get the individual objects

        public Object getObject1(){
            return this.Object1;
        }
        public Object getObject2(){
            return this.Object2;
        }
}
