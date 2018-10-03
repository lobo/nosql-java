package example;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import helper.EdgeProp;
import helper.VertexProp;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class GraphXAppMain2 {

    public static void main(String[] args) throws ParseException {

        SparkConf spark = new SparkConf().setAppName("Exercise 3");
        JavaSparkContext sparkContext= new JavaSparkContext(spark);


//        List<Tuple2<Object, VertexProp>> vertices = LoadVertices();
//        List<Tuple2<Object, VertexProp>> vertices =  (List<Tuple2<Object, VertexProp>>) sparkContext.objectFile(args[0]);

        // The main code converts them to RDDs:
//        JavaRDD<Tuple2<Object, VertexProp>> verticesRDD = sparkContext.parallelize(vertices);
          JavaRDD<Tuple2<Object, VertexProp>> verticesRDD = sparkContext.objectFile(args[0]);

//        List<Edge<EdgeProp>> edges = LoadEdges();
//        List<Edge<EdgeProp>> edges = (JavaRDD<Edge<EdgeProp>>) sparkContext.objectFile(args[0]);

        // The main code converts them to RDDs:
//        JavaRDD<Edge<EdgeProp>> edgesRDD = sparkContext.parallelize(edges);
        JavaRDD<Edge<EdgeProp>> edgesRDD = sparkContext.objectFile(args[1]);


        // Once we have created the two RDDs, vertices and edges, we can invoke the method for
        // creating the graph. Graph class is a parametrized Generic class. Its two parameters correspond to the
        // vertices and edges class, respectively.
        scala.reflect.ClassTag<VertexProp> vertexTag =
                scala.reflect.ClassTag$.MODULE$.apply(VertexProp.class);

        scala.reflect.ClassTag<EdgeProp> edgesTag =
                scala.reflect.ClassTag$.MODULE$.apply(EdgeProp.class);


        Graph<VertexProp, EdgeProp> myGraph = Graph.apply(
                verticesRDD.rdd(),
                edgesRDD.rdd(),
                null,
                StorageLevel.MEMORY_ONLY(),
                StorageLevel.MEMORY_ONLY(),
                vertexTag,
                edgesTag );


        // in the driver console:
        System.out.println("vertices:");
        myGraph.vertices().toJavaRDD().collect().forEach(System.out::println);


        System.out.println("edges:");
        myGraph.edges().toJavaRDD().collect().forEach(System.out::println);


        // ADDING PERSISTENCE BELOW:
        myGraph.vertices().toJavaRDD().saveAsObjectFile("outputvertices-ex4.dat");
        myGraph.edges().toJavaRDD().saveAsObjectFile("outputedges-ex4.dat");

        sparkContext.close();

    }


    public static List<Tuple2<Object, VertexProp>> LoadVertices()
    {
        List<Tuple2<Object, VertexProp>> vertices =
                new ArrayList<>();

        for (long rec=0; rec <= 9; rec++) {
            if ( rec % 2 ==0 ) {
                vertices.add(new Tuple2<Object, VertexProp>( rec, new VertexProp(rec + ".html", "A")));
            } else {
                vertices.add(new Tuple2<Object, VertexProp>( rec, new VertexProp(rec + ".html", "L")));
            }
        }

        return vertices;
    }


    public static List<Edge<EdgeProp>> LoadEdges() throws ParseException {
        ArrayList<Edge<EdgeProp>> edges = new ArrayList<Edge<EdgeProp>>();

        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");

        edges.add(new Edge<EdgeProp>(1L, 0L, new EdgeProp("refersTo", sdf.parse("10/10/2010"))));
        edges.add(new Edge<EdgeProp>(1L, 2L, new EdgeProp("refersTo", sdf.parse("10/10/2010"))));
        edges.add(new Edge<EdgeProp>(2L, 0L, new EdgeProp("refersTo", sdf.parse("10/10/2010"))));
        edges.add(new Edge<EdgeProp>(3L, 2L, new EdgeProp("refersTo", sdf.parse("10/10/2010"))));
        edges.add(new Edge<EdgeProp>(3L, 6L, new EdgeProp("refersTo", sdf.parse("15/10/2010"))));
        edges.add(new Edge<EdgeProp>(4L, 3L, new EdgeProp("refersTo", sdf.parse("15/10/2010"))));
        edges.add(new Edge<EdgeProp>(5L, 4L, new EdgeProp("refersTo", sdf.parse("21/10/2010"))));
        edges.add(new Edge<EdgeProp>(6L, 4L, new EdgeProp("refersTo", sdf.parse("10/10/2010"))));
        edges.add(new Edge<EdgeProp>(7L, 6L, new EdgeProp("refersTo", sdf.parse("10/10/2010"))));
        edges.add(new Edge<EdgeProp>(7L, 8L, new EdgeProp("refersTo", sdf.parse("15/10/2010"))));
        edges.add(new Edge<EdgeProp>(8L, 5L, new EdgeProp("refersTo", sdf.parse("21/10/2010"))));
        edges.add(new Edge<EdgeProp>(8L, 6L, new EdgeProp("refersTo", sdf.parse("15/10/2010"))));

        return edges;
    }



}