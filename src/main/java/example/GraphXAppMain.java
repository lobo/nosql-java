package example;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class GraphXAppMain {

    public static void main(String[] args) {

        SparkConf spark = new SparkConf().setAppName("Exercise 1");
        JavaSparkContext sparkContext= new JavaSparkContext(spark);


        List<Tuple2<Object, String>> vertices = LoadVertices();

        // The main code converts them to RDDs:
        JavaRDD<Tuple2<Object, String>> verticesRDD = sparkContext.parallelize(vertices);

        List<Edge<String>> edges = LoadEdges();

        // The main code converts them to RDDs:
        JavaRDD<Edge<String>> edgesRDD = sparkContext.parallelize(edges);


        // Once we have created the two RDDs, vertices and edges, we can invoke the method for
        // creating the graph. Graph class is a parametrized Generic class. Its two parameters correspond to the
        // vertices and edges class, respectively.
        scala.reflect.ClassTag<String> stringTag =
                scala.reflect.ClassTag$.MODULE$.apply(String.class);


        Graph<String, String> myGraph = Graph.apply(
                verticesRDD.rdd(),
                edgesRDD.rdd(),
                "default",
                StorageLevel.MEMORY_ONLY(),
                StorageLevel.MEMORY_ONLY(),
                stringTag,
                stringTag );

        myGraph.edges().toJavaRDD().foreach(
                new VoidFunction<Edge<String>>() {
                    public void call(Edge<String> t) throws Exception {
                        System.out.println(t);
                    }
                });

        Graph<String, String> myGraph2 = myGraph.reverse();

        myGraph2.edges().toJavaRDD().foreach(
                new VoidFunction<Edge<String>>() {
                    public void call(Edge<String> t) throws Exception {
                        System.out.println(t);
                    }
                });


        myGraph2.vertices().toJavaRDD().foreach(new VoidFunction<Tuple2<Object, String>>() {
            @Override
            public void call(Tuple2<Object, String> objectStringTuple2) throws Exception {
                System.out.println(objectStringTuple2);
            }
        });




        // in the driver console:
        System.out.println("vertices:");
        myGraph.vertices().toJavaRDD().collect().forEach(System.out::println);
        System.out.println("-----");
        myGraph2.vertices().toJavaRDD().collect().forEach(System.out::println);


        System.out.println("edges:");
        myGraph.edges().toJavaRDD().collect().forEach(System.out::println);
        System.out.println("-----");
        myGraph2.edges().toJavaRDD().collect().forEach(System.out::println);

        sparkContext.close();

    }


    public static List<Tuple2<Object, String>> LoadVertices()
    {
        List<Tuple2<Object, String>> vertices=
                new ArrayList<Tuple2<Object, String>>();

        for (long rec=0; rec <= 9; rec++) {
            vertices.add(new Tuple2<Object, String>( rec, rec  + ".html"));
        }

        return vertices;
    }


    public static List<Edge<String>> LoadEdges()
    {
        ArrayList<Edge<String>> edges = new ArrayList<Edge<String>>();

        edges.add(new Edge<String>(1L, 0L, "refersTo"));
        edges.add(new Edge<String>(1L, 2L, "refersTo"));
        edges.add(new Edge<String>(2L, 0L, "refersTo"));
        edges.add(new Edge<String>(3L, 2L, "refersTo"));
        edges.add(new Edge<String>(3L, 6L, "refersTo"));
        edges.add(new Edge<String>(4L, 3L, "refersTo"));
        edges.add(new Edge<String>(5L, 4L, "refersTo"));
        edges.add(new Edge<String>(6L, 4L, "refersTo"));
        edges.add(new Edge<String>(7L, 6L, "refersTo"));
        edges.add(new Edge<String>(7L, 8L, "refersTo"));
        edges.add(new Edge<String>(8L, 5L, "refersTo"));
        edges.add(new Edge<String>(8L, 6L, "refersTo"));

        return edges;
    }



}