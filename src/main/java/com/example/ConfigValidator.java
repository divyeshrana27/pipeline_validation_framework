package com.example;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.sedona.sql.utils.Adapter;
import org.apache.sedona.sql.utils.SedonaSQLRegistrator;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
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

import afu.org.checkerframework.checker.fenum.qual.SwingCompassDirection;
import afu.org.checkerframework.checker.units.qual.C;


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

    private void writeToReport(String message){
        this.report.println(message + "\n");
        this.report.flush();
    }

    private void generateReportOfInvalidData(Dataset<Row> invalidDf, String dirName){
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
        this.df = Adapter.toDf(this.spatialRDD,
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
            System.out.println("Error");
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
                        writeToReport(String.format("Error: Type mismatch for column %s", featureName));
                    }
                }
            }
        }
    }

    private Boolean checkJSONStructure(String featureName){
        this.sparkSession.udf().register("isValidJsonStructure", new ValidJSONCheck(), DataTypes.BooleanType);

        Dataset<Row> invalidDf = this.df.filter(
            functions.not(
            functions.when(functions.not(functions.isnull(functions.col(featureName))), 
            functions.callUDF("isValidJsonStructure", functions.col(featureName)))
            ));
        // get those rows which failed here
        Boolean verdict = invalidDf.count() == 0;
        if(verdict == false){
            generateReportOfInvalidData(invalidDf, "report/invalidJSON_"+featureName);
        }
        return verdict;
    }

    // validate json of all features from config json
    private void JSONValidation(){
        if(this.config.has("attributes") && this.config.getJSONObject("attributes").has("dtypes")){
            JSONObject dtypes = this.config.getJSONObject("attributes").getJSONObject("dtypes");
            if(dtypes.has("json")){
                JSONArray featureNames = dtypes.getJSONArray("json");
                for (int i = 0; i < featureNames.length(); i++) {
                    String featureName = featureNames.getString(i);
                    if(!checkJSONStructure(featureName)){
                        writeToReport(String.format("Error: Invalid json structure for column %s", featureName));
                    }
                }
            }
        }
    }

    private Object getValue(JSONObject obj, String featureName, String key){
        String featureNameDtype = getDataTypeFor(featureName).simpleString();
        switch (featureNameDtype) {
            case "long":
                return obj.getLong(key);
            case "double":
                return obj.getDouble(key);

            case "float":
                return obj.getFloat(key);

            case "integer":
                return obj.getInt(key);
            
            case "boolean":
                return obj.getBoolean(key);

            default:
                return obj.getString(key);
        }
    }

    private Object getValue(JSONArray arr, String featureName, Integer idx){
        String featureNameDtype = getDataTypeFor(featureName).simpleString();
        switch (featureNameDtype) {
            case "bigint":
                return arr.getLong(idx);
            case "double":
                return arr.getDouble(idx);

            case "float":
                return arr.getFloat(idx);

            case "integer":
                return arr.getInt(idx);
            
            case "boolean":
                return arr.getBoolean(idx);

            default:
                return arr.getString(idx);
        }
    }

    private Boolean inclusiveRangeValidation(String featureName, Object lower, Object upper){
        Dataset<Row> invalidDf = this.df.filter(
            functions.not(
            functions.when(functions.not(functions.isnull(functions.col(featureName))), 
            functions.col(featureName).geq(lower).and(functions.col(featureName).leq(upper)))
            )
        );
        // get those rows which failed here
        Boolean verdict = invalidDf.count() == 0;
        if(verdict == false){
            generateReportOfInvalidData(invalidDf, "report/invalidInclusiveRange_"+featureName);
        }
        return verdict;
    }

    private Boolean exclusiveRangeValidation(String featureName, Object lower, Object upper){
        Dataset<Row> invalidDf = this.df.filter(
            functions.not(
            functions.when(functions.not(functions.isnull(functions.col(featureName))), 
            functions.col(featureName).lt(lower).or(functions.col(featureName).gt(upper)))
            )
        );
        // get those rows which failed here
        Boolean verdict = invalidDf.count() == 0;
        if(verdict == false){
            generateReportOfInvalidData(invalidDf, "report/invalidInclusiveRange_"+featureName);
        }
        return verdict;
    }

    private void rangesValidation(){
        if(this.config.has("attributes") && this.config.getJSONObject("attributes").has("ranges")){
            JSONObject ranges = this.config.getJSONObject("attributes").getJSONObject("ranges");
            if(ranges.has("inclusive")){
                JSONObject inclusive = ranges.getJSONObject("inclusive");
                for (String featureName : inclusive.keySet()) {
                    Object lower = getValue(inclusive.getJSONArray(featureName), featureName, 0);
                    Object upper = getValue(inclusive.getJSONArray(featureName), featureName, 1);
                    inclusiveRangeValidation(featureName, lower, upper);
                }
            }
            if(ranges.has("exclusive")){
                JSONObject exclusive = ranges.getJSONObject("exclusive");
                for (String featureName : exclusive.keySet()) {
                    Object lower = getValue(exclusive.getJSONArray(featureName), featureName, 0);
                    Object upper = getValue(exclusive.getJSONArray(featureName), featureName, 1);
                    exclusiveRangeValidation(featureName, lower, upper);
                }
            }
        }
    }

    private Boolean equalValueValidation(String featureName, Object val){
        Dataset<Row> invalidDf = this.df.filter(
            functions.not(
            functions.when(functions.not(functions.isnull(functions.col(featureName))), 
            functions.col(featureName).equalTo(val))
            )
        );
        // get those rows which failed here
        Boolean verdict = invalidDf.count() == 0;
        if(verdict == false){
            generateReportOfInvalidData(invalidDf, "report/invalidEqualVal_"+featureName);
        }
        return verdict;
    }

    private Boolean notEqualValueValidation(String featureName, Object val){
        Dataset<Row> invalidDf = this.df.filter(
            functions.not(
            functions.when(functions.not(functions.isnull(functions.col(featureName))), 
            functions.col(featureName).notEqual(val))
            )
        );
        // get those rows which failed here
        Boolean verdict = invalidDf.count() == 0;
        if(verdict == false){
            generateReportOfInvalidData(invalidDf, "report/invalidNotEqualVal_"+featureName);
        }
        return verdict;
    }

    private void valuesValidation(){
        if(this.config.has("attributes") && this.config.getJSONObject("attributes").has("values")){
            JSONObject values = this.config.getJSONObject("attributes").getJSONObject("values");
            if(values.has("equal")){
                JSONObject equal = values.getJSONObject("equal");
                for (String featureName : equal.keySet()) {
                    Object val = getValue(equal, featureName, featureName);
                    equalValueValidation(featureName, val);
                }
            }
            if(values.has("not_equal")){
                JSONObject notEqual = values.getJSONObject("not_equal");
                for (String featureName : notEqual.keySet()) {
                    Object val = getValue(notEqual, featureName, featureName);
                    notEqualValueValidation(featureName, val);
                }
            }
        }
    }

    

    public void validate() {
        dtypesValidation();

        JSONValidation();

        rangesValidation();

        valuesValidation();

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
