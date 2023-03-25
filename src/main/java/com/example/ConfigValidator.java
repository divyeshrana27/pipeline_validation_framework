package com.example;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.annotation.meta.When;
import javax.swing.text.StyledEditorKit.BoldAction;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.LI;
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.sedona.sql.utils.Adapter;
import org.apache.sedona.sql.utils.SedonaSQLRegistrator;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.json.JSONArray;
import org.json.JSONObject;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import com.example.udfutils.ValidJSONCheck;

import scala.Array;
import scala.collection.Seq;


/* Currently only focus on shapefile */


public class ConfigValidator {
    private JSONObject config;
    private String inputFilePath, reportPath;
    private SparkSession sparkSession;
    private JavaSparkContext sc;
    private SpatialRDD<Geometry> spatialRDD;
    private Dataset<Row> df;
    private FeatureSource<SimpleFeatureType, SimpleFeature> featureSource;
    private Map<String, DataType> dtypesMapStringToDatatype;
    private PrintWriter report;
    // to check the types we can use geotools as they are useful instead of sedona.
    // then we can convert to dataframe from sedona, then we can cast the types.
    // after that we have a choice there, use dataframe for filteration or maybe use
    // rdd.. we have a choice so its fine.

    // first step is to convert the data types and accordingly casting the columns

    private DataType getDataTypeFor(String featureName){
        DataType featureNameType = this.df.schema().apply(featureName).dataType();
        return featureNameType;
    }

    private void dtypesCorrection() {
        if(this.featureSource == null){
            System.out.println("Error");
            return;
        }
        List<AttributeDescriptor> attributesDescriptors = this.featureSource.getSchema().getAttributeDescriptors();
        
        for (int i = 0; i < attributesDescriptors.size(); i++) {
            AttributeDescriptor attributeDescriptor = attributesDescriptors.get(i);
            String featureName = attributeDescriptor.getName().toString();
            String featureNameType = attributeDescriptor.getType().getName().toString();
            if(featureName.equals("the_geom")){
                continue;
            }
            else{
                if(this.dtypesMapStringToDatatype.get(featureNameType).equals(getDataTypeFor(featureName)))continue;
                this.df = this.df.withColumn(featureName, functions.col(featureName).cast(this.dtypesMapStringToDatatype.get(featureNameType)));
            }
        }
    }

    private void emptyToNullInDF(){
        this.df = this.df.select(
            Arrays.stream(this.df.columns())
            .map(col -> {
                if(col.equals("geometry")){
                    return functions.col(col);
                }
                return functions.when(functions.trim(functions.col(col)).equalTo(""), null).otherwise(functions.col(col)).alias(col);
            })
            .toArray(Column[]::new)
        );
    }

    public ConfigValidator(JSONObject config, String inputFilePath, String reportPath) {
        this.config = config;
        this.inputFilePath = inputFilePath;
        this.reportPath = reportPath;
        try {
            this.report = new PrintWriter(new FileWriter(this.reportPath));
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.sparkSession = SparkSession.builder()
                .appName("validation")
                .master("local[*]")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();
        this.sparkSession.sparkContext().setLogLevel("ERROR");
        SedonaSQLRegistrator.registerAll(this.sparkSession);
        this.sc = new JavaSparkContext(this.sparkSession.sparkContext());
        this.spatialRDD = ShapefileReader.readToGeometryRDD(sc, inputFilePath);
        this.df = Adapter.toDf(ShapefileReader.readToGeometryRDD(sc, inputFilePath),
                sparkSession);

        // For dtypes..
        File file = new File(this.inputFilePath);
        Map<String, Object> map = new HashMap<>();
        try {
            map.put("url", file.toURI().toURL());
            DataStore dataStore = DataStoreFinder.getDataStore(map);
            String typeName = dataStore.getTypeNames()[0];
            this.featureSource = dataStore.getFeatureSource(typeName);
        }catch(Exception e){
            this.featureSource = null;
            System.out.println("Errorrrr");
        }
        // dtypesmap convert featuresource types to datatypes.types dynamically
        this.dtypesMapStringToDatatype = new HashMap<>();
        this.dtypesMapStringToDatatype.put("Boolean",DataTypes.BooleanType);
        this.dtypesMapStringToDatatype.put("Double",DataTypes.DoubleType);
        this.dtypesMapStringToDatatype.put("Float",DataTypes.FloatType);
        this.dtypesMapStringToDatatype.put("Integer",DataTypes.IntegerType);
        this.dtypesMapStringToDatatype.put("Long",DataTypes.LongType);
        this.dtypesMapStringToDatatype.put("String",DataTypes.StringType);
        
        emptyToNullInDF();

        dtypesCorrection();
    }

    private void dtypesValidation(){
        if(this.config.has("attributes") && this.config.getJSONObject("attributes").has("dtypes")){
            JSONObject dtypes = this.config.getJSONObject("attributes").getJSONObject("dtypes");
            for (String dtype : dtypes.keySet()) {
                if(dtype.equals("json"))continue;
                JSONArray featureNames = dtypes.getJSONArray(dtype);
                for (int i = 0; i < featureNames.length(); i++) {
                    String featureName = featureNames.getString(i);
                    DataType featureNameType = getDataTypeFor(featureName);
                    if(!this.dtypesMapStringToDatatype.get(dtype).equals(featureNameType)){
                        // update report here   
                        report.printf("Error: Type mismatch for column %s", featureName);
                    }
                }
            }
        }
    }

    private void updateReport(Dataset<Row> invalidDf, String dirName){
        // List<String> columnNames = Arrays.asList(invalidDf.schema().fieldNames());

        invalidDf.toJavaRDD().map(row -> {
            List<String> vals = new ArrayList<>();
            for (int i = 0; i < row.length(); i++) {
                if(row.get(i) == null){
                    vals.add("null");
                }
                else vals.add(row.get(i).toString());
            }
            return String.join("\t", vals);
        }).saveAsTextFile(dirName);

        // for (String column : columnNames) {
        //     invalidDf = invalidDf.withColumn(column, functions.col(column).cast(DataTypes.StringType));
        // }

        // invalidDf = invalidDf.withColumn(
        //                     "data",
        //                     functions.expr(String.format("concat_ws('\t', %s)", String.join(",", columnNames)))
        //                 );
                    
        // invalidDf.select("data").write().mode(SaveMode.Append).text("file");

    }   

    private Boolean checkJSONStructure(String featureName){
        this.sparkSession.udf().register("isValidJsonStructure", new ValidJSONCheck(), DataTypes.BooleanType);
        
        Dataset<Row> temp = this.df.select("Condition").filter(functions.not(functions.isnull(functions.col("Condition"))));
        System.out.println("count - " + "Condition" + " - " + temp.count());
        Dataset<Row> invalidDf = this.df.filter(functions.not(
            functions.when(functions.not(functions.isnull(functions.col(featureName))), 
            functions.callUDF("isValidJsonStructure", functions.col(featureName)))
            ));
        // get those rows which failed here
        Boolean verdict = invalidDf.count() == 0;
        System.out.println("count - " + featureName + " - " + invalidDf.count());
        if(verdict == false){
            report.printf("Error: Invalid json structure found in column %s", featureName);
            updateReport(invalidDf, "report/invalidJSON_"+featureName);
        }
        return verdict;
    }

    private void JSONValidation(){
        if(this.config.has("attributes") && this.config.getJSONObject("attributes").has("dtypes")){
            JSONObject dtypes = this.config.getJSONObject("attributes").getJSONObject("dtypes");
            if(dtypes.has("json")){
                JSONArray featureNames = dtypes.getJSONArray("json");
                for (int i = 0; i < featureNames.length(); i++) {
                    String featureName = featureNames.getString(i);
                    if(!checkJSONStructure(featureName)){
                        report.printf("Error: Invalid json structure for column %s", featureName);
                    }
                }
            }
        }
    }

    public void validate() {
        dtypesValidation();

        JSONValidation();

        // System.out.println("\n\n\n\n");
        // UDF1<String, Boolean> isValidJsonStructure = new UDF1<String, Boolean>() {
        //     @Override
        //     public Boolean call(String value) throws Exception{
        //         System.out.println(value);
        //         return value == "0";
        //     }
        // };
        // this.sparkSession.udf().register("isValidJsonStructure", new MyUDF(), DataTypes.BooleanType);
        // Dataset<Row> res = this.df.withColumn("isValidJSON", functions.callUDF("isValidJsonStructure", functions.col("bboxes")));
        // org.apache.spark.sql.types.StructType jsonSchema = new org.apache.spark.sql.types.StructType()
        // .add("src", DataTypes.StringType);

        // Dataset<Row> res = this.df.withColumn("isValidJSON", functions.from_json(functions.col("images"), jsonSchema));
        
        // res.select("isValidJSON").show(10);

        // System.out.println(this.df.select("lat").getRows(1, 0));
        // System.out.println(this.df.collectAsList().get(0));
        // Dataset<Row> grouped = res.agg(functions.collect_list("isValidJSON").alias("valuesArray"));

        // Print the resulting DataFrame
        // System.out.println(this.df.collect());
        // this.df.show(10);
        // System.out.println(spatialRDD.rawSpatialRDD.map(obj -> {return
        // obj.getUserData();}));
        // df.printSchema();
        // df = df.withColumn("lat", functions.col("lat").cast(DataTypes.DoubleType));
        // // df.show(5);
        // this.df.printSchema();

        // JavaRDD<Row> jr = df.toJavaRDD();
        // System.out.println(jr.first().get(9).getClass());
        // System.out.println(jr.ma);
        
        System.out.println("\n\n\n\n");
        // DONE !!!
    }

}
