package org.shine;

// ubuntu > cd ../../mnt/d/FSS/ApacheFlink/Setup/flink-1.18.0 > ./bin/start-cluster.sh
// FoldFunction not exist

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FoldOperation {
    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> data = env.readTextFile("/mnt/d/FSS/ApacheFlink/Udemy/reduce-operator_flink_version_1.12/avg");

        // month, product, category, profit, count
        DataStream <Tuple5< String, String, String, Integer, Integer >> mapped =
                data.map(new Splitter());
        // tuple  [June,Category5,Bat,12,1]
        //        [June,Category4,Perfume,10,1]

        // groupBy 'month'
        DataStream < Tuple4 < String, String, Integer, Integer >> folded =
                mapped.keyBy(0).fold(new Tuple4<String, String, Integer, Integer>("", "", 0, 0), new Fold1());
        // June { [Category5,Bat,12,1] Category4,Perfume,10,1}	//rolling reduce
        // reduced = { [Category4,Perfume,22,2] ..... }
        // month, avg. profit
        DataStream <Tuple2< String, Double >> profitPerMonth = folded.map(new MapFunction < Tuple4 < String, String, Integer, Integer > , Tuple2 < String, Double >> () {
            public Tuple2 < String, Double > map(Tuple4 < String, String, Integer, Integer > input) {
                return new Tuple2 < String, Double > (input.f0, new Double((input.f2 * 1.0) / input.f3));
            }
        });

        //profitPerMonth.print();
        profitPerMonth.writeAsText("/mnt/d/FSS/ApacheFlink/Udemy/reduce-operator_flink_version_1.12/profit_per_month_fold.txt");

        // execute program
        env.execute("Avg Profit Per Month by Fold Operation");

    }

    public static class Fold1 implements FoldFunction<
            Tuple5 < String, String, String, Integer, Integer >,
            Tuple4 < String, String, Integer, Integer >
            > {
        public Tuple4 < String, String, Integer, Integer > fold(Tuple4 < String, String, Integer, Integer > defaultIn,
                                                                Tuple5 < String, String, String, Integer, Integer > current)
        {
            defaultIn.f0 = current.f0;
            defaultIn.f1 = current.f1;
            defaultIn.f2 += current.f3;
            defaultIn.f3 += current.f4;
            return defaultIn;
//            return new Tuple4 < String, String, Integer, Integer > (current.f0, current.f1, defaultIn.f2 + current.f3, defaultIn.f3 + current.f4);
        }
    }

    public static class Splitter implements MapFunction< String, Tuple5 < String, String, String, Integer, Integer >> {
        public Tuple5 < String, String, String, Integer, Integer > map(String value) // 01-06-2018,June,Category5,Bat,12
        {
            String[] words = value.split(","); // words = [{01-06-2018},{June},{Category5},{Bat}.{12}
            // ignore timestamp, we don't need it for any calculations
            return new Tuple5 < String, String, String, Integer, Integer > (words[1], words[2], words[3], Integer.parseInt(words[4]), 1);
        } //    June    Category5      Bat                      12
    }
}
