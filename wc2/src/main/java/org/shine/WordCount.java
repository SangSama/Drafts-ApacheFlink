package org.shine;

// ubuntu > cd ../../mnt/d/FSS/ApacheFlink/Setup/flink-1.18.0 > ./bin/start-cluster.sh
// ./bin/flink run ../../Udemy/study-apache-flink/wc2/target/udemy-flink-1.0-SNAPSHOT.jar --input file:///mnt/d/FSS/ApacheFlink/Udemy/study-apache-flink/data/s02/wc_1/wc.txt --output file:///mnt/d/FSS/ApacheFlink/Udemy/study-apache-flink/data/s02/output/wc_1_out

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

public class WordCount {

    public static void main(String[] args) throws Exception
    {
        // set up the execution environment
        // getExecutionEnvironment() :
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        final ParameterTool params = ParameterTool.fromArgs(args);

        // setGlobalJobParameters() : đặt biến cục bộ
        env.getConfig().setGlobalJobParameters(params);

        // read the text file from given input path
        // readTextFile() : độc tệp theo dòng, mỗi lần một dòng, trả về chuỗi
        DataSet<String> text = env.readTextFile(params.get("input"));

        // filter all the names starting with N
        // FilterFunction() : tham số truyền vào là kiểu dữ liệu
//        DataSet<String> filtered = text.filter(new FilterFunction<String>()
//
//        {
//            public boolean filter(String value)
//            {
//                return value.startsWith("N"); // true -> saved
//            }
//        });
        // Returns a tuple of (name,1)
        DataSet<String> filtered = text.filter(new MyFilter());

        // map()
//        DataSet<Tuple2<String, Integer>> tokenized = filtered.map(new Tokenizer());

        //flatmap()
        DataSet<Tuple2<String, Integer>> tokenized2 = text.flatMap(new Tokenizer2());

        // group by the tuple field "0" and sum up tuple field "1"
//        DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(new int[] { 0 }).sum(1);
        DataSet<Tuple2<String, Integer>> counts = tokenized2.groupBy(new int[] { 0 }).sum(1);

        // emit result
        if (params.has("output"))
        {
            counts.writeAsCsv(params.get("output"), "\n", " ");
            // execute program
            env.execute("WordCount Example");
        }

    }

    public static final class MyFilter implements FilterFunction<String> {
        public boolean filter(String value)
        {
            return value.startsWith("N"); // true -> saved
        }
    }

    public static final class Tokenizer implements MapFunction<String, Tuple2<String, Integer>>
    {
        public Tuple2<String, Integer> map(String value)
        {
            return new Tuple2(value, Integer.valueOf(1));
        }
    }

    public static final class Tokenizer2 implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            //split the line
            String[] tokens = value.split(" "); // tokens =
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(Tuple2.of(token, 1));
                }
            }
        }
    }

}
