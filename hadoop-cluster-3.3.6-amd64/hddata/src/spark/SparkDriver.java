/*
 * Copyright 2025 Emanuele Relmi (https://github.com/Kirito-Emo)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package spark;

import java.util.Arrays;
import java.util.Locale;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

/**
 * For each car "age group" (grouped by Year) find the best-selling Model.
 * - Input: CSV with header: Model, Year, Region, Color, Fuel_Type, Transmission, Engine_Size_L, Mileage_KM, Price_USD, Sales_Volume, Sales_Classification
 * - Output: lines "ageGroup \t model \t totalVolume"
 *
 * Steps (RDD):
 * 1) textFile -> filter header -> mapToPair((ageGroup, model), volume)
 * 2) reduceByKey(sum volumes)
 * 3) map to (ageGroup, (model, totalVol))
 * 4) reduceByKey keep max by totalVol
 * 5) saveAsTextFile
 */
public class SparkDriver
{
    /**
     * Entry point for Spark job.
     * @param args  inputPath outputDir
     */
    public static void main(String[] args)
    {
        if (args.length < 2)
        {
            System.err.println("Usage: SparkDriver <inputPath> <outputDir>");
            System.exit(1);
        }
        final String inputPath = args[0];
        final String outputDir = args[1];

        // Spark configuration
        SparkConf conf = new SparkConf().setAppName("BMW AgeGroup Top Model");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read all lines from CSV
        JavaRDD<String> lines = sc.textFile(inputPath);

        // Filter header and blank lines
        JavaRDD<String> data = lines.filter(s -> {
            if (s == null)
                return false;

            String t = s.trim();
            if (t.isEmpty())
                return false;

            return !t.toLowerCase(Locale.ROOT).startsWith("model,");
        });

        // Map -> Pair: key=(ageGroup, model), value=volume
        JavaPairRDD<Tuple2<String, String>, Integer> pair = data.mapToPair(
                new PairFunction<String, Tuple2<String, String>, Integer>()
                {
                    @Override
                    public Tuple2<Tuple2<String, String>, Integer> call(String s)
                    {
                        // Splitting CSV by commas
                        String[] f = s.split(",", -1);

                        // Guard value
                        if (f.length < 11)
                            return new Tuple2<>(new Tuple2<>("invalid","invalid"), 0);

                        String model = f[0].trim(); // Model
                        String years = f[1].trim(); // Year
                        String vols  = f[9].trim(); // Sales volume

                        int year = safeInt(years);
                        int vol  = safeInt(vols);

                        String ageGroup = bucketYear(year);
                        return new Tuple2<>(new Tuple2<>(ageGroup, model), vol);
                    }

                    private int safeInt(String x)
                    {
                        try
                        {
                            return Integer.parseInt(x);
                        }
                        catch(Exception e)
                        {
                            return 0;
                        }
                    }

                    // Year bucketing
                    private String bucketYear(int year)
                    {
                        if (year <= 2014)
                            return "age<=2014";

                        if (year <= 2018)
                            return "2015_2018";

                        if (year <= 2021)
                            return "2019_2021";

                        return ">=2022";
                    }
                });

        // Reduce volumes per (ageGroup, model)
        JavaPairRDD<Tuple2<String,String>, Integer> reduced = pair.reduceByKey((a, b) -> a + b);

        // Map to (ageGroup, (model, totalVol))
        JavaPairRDD<String, Tuple2<String,Integer>> byAge = reduced.mapToPair(t -> new Tuple2<>(t._1._1, new Tuple2<>(t._1._2, t._2)));

        // For each ageGroup, keep the max by totalVol
        JavaPairRDD<String, Tuple2<String,Integer>> topPerAge = byAge.reduceByKey((a, b) -> (a._2 >= b._2) ? a : b);

        // Save as text with the format "ageGroup \t model \t totalVolume"
        JavaRDD<String> out = topPerAge.map(t -> t._1 + "\t" + t._2._1 + "\t" + t._2._2);

        out.saveAsTextFile(outputDir);
        sc.stop();
    }
}