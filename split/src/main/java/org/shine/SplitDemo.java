package org.shine;

// ubuntu > cd ../../mnt/d/FSS/ApacheFlink/Setup/flink-1.18.0 > ./bin/start-cluster.sh
// run: ./bin/flink run ../../Udemy/study-apache-flink/split/target/split-flink-1.0-SNAPSHOT.jar

import java.util.List;
import java.util.ArrayList;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.SplitStream;
//import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.streaming.api.functions.ProcessFunction;

public class SplitDemo {
    public static void main(String[] args) throws Exception {

        // set up the stream execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataStream < String > text = env.readTextFile("/mnt/d/FSS/ApacheFlink/Udemy/study-apache-flink/data/s03/split/oddeven");

        // String type side output for Even values
        final OutputTag < String > evenOutTag = new OutputTag < String > ("even-string-output") {};
        // Integer type side output for Odd values
        final OutputTag < Integer > oddOutTag = new OutputTag < Integer > ("odd-int-output") {};

//        SplitStream<Integer> evenOddStream = text.map(new MapFunction<String, Integer>()
//                {
//                    public Integer map(String value)
//                    {
//                        return Integer.parseInt(value);
//                    }})
//
//                .split(new OutputSelector<Integer>()
//                {
//                    public Iterable<String> select(Integer value)
//                    {
//                        List<String> out = new ArrayList<String>();
//                        if (value%2 == 0)
//                            out.add("even");              // label element  --> even 454   odd 565 etc
//                        else
//                            out.add("odd");
//                        return out;
//                    }
//                });
//
//        DataStream<Integer> evenData = evenOddStream.select("even");
//        DataStream<Integer> oddData = evenOddStream.select("odd");

        SingleOutputStreamOperator < Integer > mainStream = text
            .process(new ProcessFunction < String, Integer > () {
                @Override
                public void processElement(
                        String value,
                        Context ctx,
                        Collector < Integer > out) throws Exception {

                int intVal = Integer.parseInt(value);

                // get all data in regular output as well
                out.collect(intVal);

                if (intVal % 2 == 0) {
                    // emit data to side output for even output
                    ctx.output(evenOutTag, String.valueOf(intVal));
                } else {
                    // emit data to side output for even output
                    ctx.output(oddOutTag, intVal);
                }
                }
            });

        DataStream < String > evenSideOutputStream = mainStream.getSideOutput(evenOutTag);
        DataStream < Integer > oddSideOutputStream = mainStream.getSideOutput(oddOutTag);

        evenSideOutputStream.writeAsText("/mnt/d/FSS/ApacheFlink/Udemy/study-apache-flink/data/s03/output/even");
        oddSideOutputStream.writeAsText("/mnt/d/FSS/ApacheFlink/Udemy/study-apache-flink/data/s03/output/odd");

        // execute program
        env.execute("Split Demo : ODD EVEN");

    }

}
