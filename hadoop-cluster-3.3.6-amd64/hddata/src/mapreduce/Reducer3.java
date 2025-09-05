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
import java.util.ArrayList;
import java.util.List;

/**
 * Job 3 Reducer:
 * - Sorts items by sumVol in descending order and emits Top-K per region
 * - Output: region \t model \t sumVol \t sharePct \t avgPrice \t highShare
 */
public class Reducer3 extends Reducer<Text, Text, Text, Text>
{
    private int topK;

    @Override
    protected void setup(Context ctx)
    {
        topK = ctx.getConfiguration().getInt("top.k", 5);
    }

    @Override
    protected void reduce(Text region, Iterable<Text> values, Context ctx) throws IOException, InterruptedException
    {
        List<String[]> rows = new ArrayList<>();

        for (Text t : values)
        {
            String[] p = t.toString().split("\\t");
            if (p.length >= 5)
                rows.add(p);
        }

        rows.sort((a, b) -> Integer.compare(Integer.parseInt(b[1]), Integer.parseInt(a[1])));

        int k = 0;
        for (String[] r : rows)
        {
            if (k++ >= topK)
                break;

            ctx.write(region, new Text(r[0] + "\t" + r[1] + "\t" + r[2] + "\t" + r[3] + "\t" + r[4]));
        }
    }
}