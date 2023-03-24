package com.example;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader;
import org.apache.sedona.core.formatMapper.shapefileParser.fieldname.FieldnameInputFormat;
import org.apache.sedona.core.formatMapper.shapefileParser.shapes.ShapeFileReader;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.sedona.sql.utils.Adapter;
import org.apache.sedona.sql.utils.SedonaSQLRegistrator;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;



/**
 * Hello world!
 *
 */
public class App 
{
    
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        SparkSession spark = SparkSession.builder()
                            .appName("Read Shapefile to DataFrame")
                            .master("local[*]")
                            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                            .getOrCreate();
        System.out.println(spark);
        SedonaSQLRegistrator.registerAll(spark);
        String path = "/Users/divyesh.rana/Documents/validation/shapefile/";

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        try {
            // List<String> res = readFieldNames2(sc, path);
            SpatialRDD<Geometry> spatialRDD = ShapefileReader.readToGeometryRDD(sc, path);
            Dataset<Row> df = Adapter.toDf(spatialRDD, spark);
            df.printSchema();
            df.show(10);

        } catch (Exception e) {
            e.printStackTrace();
        }

        // Dataset<Row> df = spark.read().csv("/Users/divyesh.rana/Documents/data_pipeline/temp.csv");
        // df.printSchema();
        // df.show(10);
        System.out.println("jehferkfjdnv\n\n\n\n");
        
        // springboot and spark together gives some errors... and can not read shapefile 

    }   
}
