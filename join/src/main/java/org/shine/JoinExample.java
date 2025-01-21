package org.shine;

// ubuntu > cd ../../mnt/d/FSS/ApacheFlink/Setup/flink-1.18.0
// ./bin/flink run ../../Udemy/study-apache-flink/join/target/join-flink-1.0-SNAPSHOT.jar --input1 file:///mnt/d/FSS/ApacheFlink/Udemy/study-apache-flink/data/s02/join/person --input2 file:///mnt/d/FSS/ApacheFlink/Udemy/study-apache-flink/data/s02/join/location --output file:///mnt/d/FSS/ApacheFlink/Udemy/study-apache-flink/data/s02/output/join_output

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

@SuppressWarnings("serial")
public class JoinExample
{
    public static void main(String[] args) throws Exception
    {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool params = ParameterTool.fromArgs(args);
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // Read person file and generate tuples out of each string read
        DataSet<Tuple2<Integer, String>> personSet = env.readTextFile(params.get("input1"))
                .map(new MapFunction<String, Tuple2<Integer, String>>()                                     //presonSet = tuple of (1  John)
                {
                    public Tuple2<Integer, String> map(String value)
                    {
                        String[] words = value.split(","); // words = [ {1} {John}]
                        return new Tuple2<Integer, String>(Integer.parseInt(words[0]), words[1]);
                    }
                });
        // Read location file and generate tuples out of each string read
        DataSet<Tuple2<Integer, String>> locationSet = env.readTextFile(params.get("input2")).
                map(new MapFunction<String, Tuple2<Integer, String>>()
                {                                                                                           //locationSet = tuple of (1  DC)
                    public Tuple2<Integer, String> map(String value)
                    {
                        String[] words = value.split(","); // split trả về 1 array[]
                        return new Tuple2<Integer, String>(Integer.parseInt(words[0]), words[1]);
                    }
                });

        // ----------------------------------------------------
        // INNER JOIN = matching records from both

        // join (<=> inner-join) datasets on person_id with condition WHERE 0 = 0
        // joined format will be <id, person_name, state>
//        DataSet<Tuple3<Integer, String, String>> joined = personSet.join(locationSet).where(0) .equalTo(0)
//                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>()
//                {
//
//                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person,  Tuple2<Integer, String> location)
//                    {
//                        return new Tuple3<Integer, String, String>(person.f0,   person.f1,  location.f1);         // returns tuple of (1 John DC)
//                    }
//                });
//
//        joined.writeAsCsv(params.get("output"), "\n", " ");
//
//        env.execute("Join example");

        // ----------------------------------------------------
        // LEFT OUTER JOIN = matching records from both + non matching from left

        // left outer join datasets on person_id with condition WHERE 0 = 0
        // joined format will be <id, person_name, state>
//        DataSet<Tuple3<Integer, String, String>> joined = personSet.leftOuterJoin(locationSet).where(0) .equalTo(0)
//                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>()
//                {
//                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person,  Tuple2<Integer, String> location)
//                    {
//                        // check for nulls
//                        if (location == null)
//                        {
//                            return new Tuple3<Integer, String, String>(person.f0, person.f1, "NULL");
//                        }
//
//                        return new Tuple3<Integer, String, String>(person.f0,   person.f1,  location.f1);
//                    }
//                });
//
//        joined.writeAsCsv(params.get("output"), "\n", " ");
//
//        env.execute("Left Outer Join Example");

        // ----------------------------------------------------
        // RIGHT OUTER JOIN = matching records from both + non matching from right

        // right outer join datasets on person_id
        // joined format will be <id, person_name, state>
//        DataSet<Tuple3<Integer, String, String>> joined = personSet.rightOuterJoin(locationSet).where(0) .equalTo(0)
//                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>(){
//
//                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person,  Tuple2<Integer, String> location)
//                    {
//                        // check for nulls
//                        if (person == null)
//                        {
//                            return new Tuple3<Integer, String, String>(location.f0, "NULL", location.f1);
//                        }
//
//                        return new Tuple3<Integer, String, String>(person.f0,   person.f1,  location.f1);
//                    }
//                });//.collect();
//
//        joined.writeAsCsv(params.get("output"), "\n", " ");
//
//        env.execute("Right Outer Join Example");

        // ----------------------------------------------------
        // FULL OUTER JOIN = matching records from both + non matching from both

        // full outer join datasets on person_id
        // joined format will be <id, person_name, state>
//        DataSet<Tuple3<Integer, String, String>> joined = personSet.fullOuterJoin(locationSet).where(0) .equalTo(0)
//                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>(){
//
//                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person,  Tuple2<Integer, String> location)
//                    {
//                        // check for nulls
//                        if (location == null)
//                        {
//                            return new Tuple3<Integer, String, String>(person.f0, person.f1, "NULL");
//                        }
//                        // for rightOuterJoin
//                        else if (person == null)
//                            return new Tuple3<Integer, String, String>(location.f0, "NULL", location.f1);
//
//                        return new Tuple3<Integer, String, String>(person.f0,   person.f1,  location.f1);
//                    }
//                });
//
//        joined.writeAsCsv(params.get("output"), "\n", " ");
//
//        env.execute("Full Outer Join Example");

        // ----------------------------------------------------
        // Exclusive Feature ⇒ type of optimization technique
        // OPTIMIZER_CHOOSES : Không đưa ra gợi ý nào cả và để việc lựa chọn cho hệ thống
        // BROADCAST_HASH_FIRST :

        DataSet<Tuple3<Integer, String, String>> joined = personSet.join(locationSet,JoinHint.REPARTITION_SORT_MERGE).where(0) .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>()
                {

                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person,  Tuple2<Integer, String> location)
                    {
                        return new Tuple3<Integer, String, String>(person.f0,   person.f1,  location.f1);         // returns tuple of (1 John DC)
                    }
                });

        joined.writeAsCsv(params.get("output"), "\n", " ");

        env.execute("Join example");

    }
}
