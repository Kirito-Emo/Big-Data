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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * Job 1 Mapper:
 * - Parses CSV rows (expects header starting with "Model")
 * - Key = "region \t model"
 * - Value = "1|sumVolume|sumPrice|isHigh"
 */
public class Mapper1 extends Mapper<LongWritable, Text, Text, Text>
{
    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException
    {
        String line = value.toString().trim();
        if (line.isEmpty())
            return;

        String[] f = line.split(",", -1);
        if (f.length < 11 || "Model".equalsIgnoreCase(f[0]))
            return;

        String model  = f[0].trim().toLowerCase();
        String region = f[2].trim().toLowerCase();
        String priceS = f[8].trim();
        String volS   = f[9].trim();
        String cls    = f[10].trim();

        if (region.isEmpty() || model.isEmpty())
            return;

        int volume = safeInt(volS);
        int price  = safeInt(priceS);
        int isHigh = "high".equalsIgnoreCase(cls) ? 1 : 0;

        outKey.set(region + "\t" + model);
        outVal.set("1|" + volume + "|" + price + "|" + isHigh);
        ctx.write(outKey, outVal);
    }

    private int safeInt(String s)
    {
        try
        {
            return Integer.parseInt(s);
        }
        catch (Exception e)
        {
            return 0;
        }
    }
}