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
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Job 3 Mapper:
 * - Loads region totals (Job 2) in setup() using "step2.totals.path"
 * - Reads Job 1 lines, computes share%, avgPrice, highShare
 * - Key = region
 * - Value = "model \t sumVol \t sharePct \t avgPrice \t highShare"
 */
public class Mapper3 extends Mapper<LongWritable, Text, Text, Text>
{
    private final Map<String,Integer> regionTotals = new HashMap<>();
    private final Text outKey = new Text();
    private final Text outVal = new Text();

    /**
     * Loads region totals from the output of Job 2.
     *
     * @param ctx   Hadoop mapper context used to access configuration and filesystem
     * @throws IOException if reading the totals fails
     */
    @Override
    protected void setup(Context ctx) throws IOException
    {
        Configuration conf = ctx.getConfiguration();
        String totalsPath = conf.get("step2.totals.path");
        if (totalsPath == null)
            return; // No totals available; shares will be 0

        FileSystem fs = FileSystem.get(conf);
        Path dir = new Path(totalsPath);
        RemoteIterator<LocatedFileStatus> it = fs.listFiles(dir, false);

        while (it.hasNext())
        {
            Path p = it.next().getPath();
            if (!p.getName().startsWith("part-"))
                continue;

            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p))))
            {
                String line;

                // Each line: region \t regionTotalVolume
                while ((line = br.readLine()) != null)
                {
                    String[] kv = line.split("\\t");
                    if (kv.length >= 2)
                    {
                        try
                        {
                            regionTotals.put(kv[0], Integer.parseInt(kv[1]));
                        }
                        catch (Exception ex) {} // Ignore malformed totals; acts as if region had no total
                    }
                }
            }
        }
    }

    /**
     * Computes per-model metrics and emits (region, "model\t...metrics...").
     *
     * @param key       byte offset of the line in the input split (unused)
     * @param value     a line in the format "region \t model \t count|sumVolume|sumPrice|highCount"
     * @param ctx       Hadoop context used to emit key/value pairs
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException
    {
        String[] kv = value.toString().split("\\t");
        if (kv.length < 3)
            return;

        String region = kv[0].trim();
        String model  = kv[1].trim();
        String[] p = kv[2].split("\\|");
        if (p.length < 4)
            return;

        int count   = safeInt(p[0]);
        int sumVol  = safeInt(p[1]);
        int sumPrice= safeInt(p[2]);
        int high    = safeInt(p[3]);

        Integer tot = regionTotals.get(region);
        double share = (tot != null && tot > 0) ? (100.0 * sumVol / tot) : 0.0;
        double avgPrice  = (count > 0) ? ((double) sumPrice / count) : 0.0;
        double highShare = (count > 0) ? (100.0 * high / count) : 0.0;

        outKey.set(region);
        outVal.set(
                model + "\t"
                + sumVol + "\t"
                + String.format(java.util.Locale.US, "%.4f", share) + "\t"
                + String.format(java.util.Locale.US, "%.2f", avgPrice) + "\t"
                + String.format(java.util.Locale.US, "%.2f", highShare)
        );
        ctx.write(outKey, outVal);
    }

    /**
     * Parses a string as integer with fallback to 0.
     *
     * @param s string to parse
     * @return integer value or 0 if parsing fails
     */
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