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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * Job 1 Reducer:
 * - Final aggregate per (region, model)
 * - Outputs: key = "region \t model", value = "count|sumVolume|sumPrice|highCount"
 */
public class Reducer1 extends Reducer<Text, Text, Text, Text>
{
    private final Text outVal = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException
    {
        int c=0, vol=0, prc=0, hi=0;
        for (Text t : values)
        {
            String[] p = t.toString().split("\\|");
            if (p.length >= 4)
            {
                c   += safeInt(p[0]);
                vol += safeInt(p[1]);
                prc += safeInt(p[2]);
                hi  += safeInt(p[3]);
            }
        }

        outVal.set(c + "|" + vol + "|" + prc + "|" + hi);
        ctx.write(key, outVal);
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