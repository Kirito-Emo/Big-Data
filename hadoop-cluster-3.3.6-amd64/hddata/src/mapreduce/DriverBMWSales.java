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
package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Driver:
 * - Chains 3 jobs (Job 1, Job 2, Job 3)
 * - Passes Job 2 step2.totals.path to Job 3
 */
public class DriverBMWSales
{
    /**
     * Chains Job 1 -> Job 2 -> Job 3, wiring outputs and passing configuration for the final step.
     *
     * @param args  CLI arguments: input, out_1, out_2, out_3, [topK]
     * @throws Exception if job submission or execution fails
     */
    public static void main(String[] args) throws Exception
    {
        if (args.length < 4)
        {
            System.err.println("Usage: DriverBMWSales <input> <out_1> <out_2> <out_3> [topK=5]");
            System.exit(1);
        }

        String in = args[0];
        String o1 = args[1];
        String o2 = args[2];
        String o3 = args[3];
        int topK = (args.length > 4) ? Integer.parseInt(args[4]) : 5;

        // ---- Job 1 ----
        Configuration c1 = new Configuration();
        Job j1 = Job.getInstance(c1);
        j1.setJobName("BMW - Region/Model Aggregate");
        j1.setJarByClass(DriverBMWSales.class);
        j1.setMapperClass(Mapper1.class);
        j1.setCombinerClass(Combiner1.class); // combines component-wise sums to reduce shuffle
        j1.setReducerClass(Reducer1.class);

        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(Text.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(Text.class);

        j1.setInputFormatClass(TextInputFormat.class);
        j1.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(j1, new Path(in));
        TextOutputFormat.setOutputPath(j1, new Path(o1));
        j1.setNumReduceTasks(1);
        if (!j1.waitForCompletion(true))
            System.exit(1);

        // ---- Job 2 ----
        Configuration c2 = new Configuration();
        Job j2 = Job.getInstance(c2);
        j2.setJobName("BMW - Region Totals");
        j2.setJarByClass(DriverBMWSales.class);
        j2.setMapperClass(Mapper2.class);
        j2.setReducerClass(Reducer2.class);

        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(IntWritable.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(IntWritable.class);

        j2.setInputFormatClass(TextInputFormat.class);
        j2.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(j2, new Path(o1));
        TextOutputFormat.setOutputPath(j2, new Path(o2));
        j2.setNumReduceTasks(1);
        if (!j2.waitForCompletion(true))
            System.exit(2);

        // ---- Job 3 ----
        Configuration c3 = new Configuration();
        c3.setInt("top.k", topK);           // how many rows to emit per region
        c3.set("step2.totals.path", o2);    // where Mapper3 will load region totals from

        Job j3 = Job.getInstance(c3);
        j3.setJobName("BMW - Top-K per Region");
        j3.setJarByClass(DriverBMWSales.class);
        j3.setMapperClass(Mapper3.class);
        j3.setReducerClass(Reducer3.class);

        j3.setMapOutputKeyClass(Text.class);
        j3.setMapOutputValueClass(Text.class);
        j3.setOutputKeyClass(Text.class);
        j3.setOutputValueClass(Text.class);

        j3.setInputFormatClass(TextInputFormat.class);
        j3.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(j3, new Path(o1));
        TextOutputFormat.setOutputPath(j3, new Path(o3));
        j3.setNumReduceTasks(1);
        if(!j3.waitForCompletion(true))
            System.exit(3);
    }
}