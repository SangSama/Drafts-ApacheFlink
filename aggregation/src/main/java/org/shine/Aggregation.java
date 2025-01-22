package org.shine;

// ubuntu > cd ../../mnt/d/FSS/ApacheFlink/Setup/flink-1.18.0 > ./bin/start-cluster.sh
// run: ./bin/flink run ../../Udemy/study-apache-flink/aggregation/target/aggregation-flink-1.0-SNAPSHOT.jar

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Aggregation {
    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream < String > data = env.readTextFile("/mnt/d/FSS/ApacheFlink/Udemy/study-apache-flink/data/s03/aggregation/avg1");

        // month, category,product, profit,
        DataStream < Tuple4 < String, String, String, Integer >> mapped = data.map(new Splitter());
        // tuple  [June,Category5,Bat,12]
        //       [June,Category4,Perfume,10,1]

        mapped.keyBy(t -> t.f0).sum(3).writeAsText("/mnt/d/FSS/ApacheFlink/Udemy/study-apache-flink/data/s03/output/avg_output1");

        mapped.keyBy(t -> t.f0).min(3).writeAsText("/mnt/d/FSS/ApacheFlink/Udemy/study-apache-flink/data/s03/output/avg_output2");

        mapped.keyBy(t -> t.f0).minBy(3).writeAsText("/mnt/d/FSS/ApacheFlink/Udemy/study-apache-flink/data/s03/output/avg_output3");

        mapped.keyBy(t -> t.f0).max(3).writeAsText("/mnt/d/FSS/ApacheFlink/Udemy/study-apache-flink/data/s03/output/avg_output4");

        mapped.keyBy(t -> t.f0).maxBy(3).writeAsText("/mnt/d/FSS/ApacheFlink/Udemy/study-apache-flink/data/s03/output/avg_output5");

        // execute program
        env.execute("Aggregation Example");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    public static class Splitter implements MapFunction < String, Tuple4 < String, String, String, Integer >> {
        public Tuple4 < String, String, String, Integer > map(String value)
        // 01-06-2018,June,Category5,Bat,12
        {
            String[] words = value.split(",");
            // words = [{01-06-2018},{June},{Category5},{Bat}.{12}
            // ignore timestamp, we don't need it for any calculations

            return new Tuple4 < String, String, String, Integer > (words[1], words[2], words[3], Integer.parseInt(words[4]));
        } //    June    Category5      Bat               12
    }
}
