package com.example;

import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.sedona.sql.utils.Adapter;
import org.apache.sedona.sql.utils.SedonaSQLRegistrator;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.JSONObject;
import org.locationtech.jts.geom.Geometry;


public class App 
{
    public static String fun(String s){
        StringBuilder res = new StringBuilder();
        char lol = '"';
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if(c == '$'){
                res.append(lol);
            }
            else res.append(c);
        }
        return new String(res);
    }
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        String path = "/Users/divyesh.rana/Documents/validation/shapefile/";
        String s = "{$attributes$: {$dtypes$: {$Long$: [$Division$], $String$: [$FeatureNam$, $FeatureSta$, $Condition$, $Visible$, $Legible$, $Reflective$, $images$, $bboxes$, $geohash$, $fpath$, $RouteName$, $StreetName$, $UUID$, $RouteMaint$, $RouteID$, $BeginFeatu$, $EndFeature$, $MaintCnt$, $LocCntyC$, $RouteCla$, $RouteInv$, $Direction$, $TravelDir$, $UniqueID$, $SyncID$], $Double$: [$lat$, $lon$, $MPLength$, $Length$, $Width$, $Area$, $BeginMp1$, $EndMp1$, $MaxMp1$, $Shape_Leng$], $json$: [$bboxes$, $images$, $FeatureNam$]}, $ranges$: {$inclusive$: {$lat$: [0, 100], $lon$: [-100, 100], $Division$: [0, 100], $Length$: [0, 10000], $Width$: [0, 10000], $BeginMp1$: [0, 10000], $EndMp1$: [0, 10000], $MaxMp1$: [0, 10000], $MPLength$: [0, 10000]}, $exclusive$: {$lat$: [100, 100], $lon$: [100, 100], $Division$: [100, 100], $Length$: [10000, 10000], $Width$: [10000, 10000], $BeginMp1$: [10000, 10000], $EndMp1$: [10000, 10000], $MaxMp1$: [10000, 10000], $MPLength$: [10000, 10000]}}, $values$: {$equal$: {$RouteMaint$: $System$}, $not_equal$: {$FeatureNam$: $-1$, $FeatureSta$: $-1$, $Condition$: $-1$, $Visible$: $-1$, $Legible$: $-1$, $Reflective$: $-1$, $images$: $-1$, $bboxes$: $-1$, $geohash$: $-1$, $fpath$: $-1$, $RouteName$: $-1$, $StreetName$: $-1$, $UUID$: $-1$, $RouteMaint$: $-1$, $RouteID$: $-1$, $BeginFeatu$: $-1$, $EndFeature$: $-1$, $MaintCnt$: $-1$, $LocCntyC$: $-1$, $RouteCla$: $-1$, $RouteInv$: $-1$, $Direction$: $-1$, $TravelDir$: $-1$, $UniqueID$: $-1$, $SyncID$: $-1$}}, $subsets$: {$inclusive$: {$Condition$: [$good$, $damaged$], $Visible$: [$0$, $1$], $Legible$: [$0$, $1$], $Reflective$: [$0$, $1$]}, $exclusive$: {$FeatureNam$: [$a$, $b$], $FeatureSta$: [$a$, $b$], $Condition$: [$a$, $b$], $Visible$: [$a$, $b$], $Legible$: [$a$, $b$], $Reflective$: [$a$, $b$], $images$: [$a$, $b$], $bboxes$: [$a$, $b$], $geohash$: [$a$, $b$], $fpath$: [$a$, $b$], $RouteName$: [$a$, $b$], $StreetName$: [$a$, $b$], $UUID$: [$a$, $b$], $RouteMaint$: [$a$, $b$], $RouteID$: [$a$, $b$], $BeginFeatu$: [$a$, $b$], $EndFeature$: [$a$, $b$], $MaintCnt$: [$a$, $b$], $LocCntyC$: [$a$, $b$], $RouteCla$: [$a$, $b$], $RouteInv$: [$a$, $b$], $Direction$: [$a$, $b$], $TravelDir$: [$a$, $b$], $UniqueID$: [$a$, $b$], $SyncID$: [$a$, $b$]}}, $not_null$: [$lat$, $lon$, $images$, $bboxes$, $geohash$, $fpath$, $Length$, $Width$, $Area$]}, $geometry$: {$types$: $Point$}}";
        s = fun(s);
        // String json = "{$attributes$:{$dtypes$:{$String$:[$FeatureNam$,$lat$]}}}";
        JSONObject obj = new JSONObject(s);
        // System.out.println(obj);
        ConfigValidator configValidator = new ConfigValidator(obj, path, "report.txt");
        configValidator.validate();
    }   
}
