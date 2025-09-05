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

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * Job 2 Mapper:
 * - Reads Job 1 lines: "region \t model \t count|sumVolume|sumPrice|highCount"
 * - Key = region
 * - Value = sumVolume
 */
public class Mapper2 extends Mapper<LongWritable, Text, Text, IntWritable>
{
    private final Text outKey = new Text();
    private final IntWritable outVal = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException
    {
        String[] kv = value.toString().split("\\t");
        if (kv.length < 3)
            return;

        String region = kv[0].trim();
        String payload = kv[2].trim();
        String[] p = payload.split("\\|");
        if (p.length < 2)
            return;

        int sumVolume = safeInt(p[1]);
        outKey.set(region);
        outVal.set(sumVolume);
        ctx.write(outKey, outVal);
    }

    private int safeInt(String s)
    {
        try
        {
            return Integer.parseInt(s);
        }
        catch(Exception e)
        {
            return 0;
        }
    }
}