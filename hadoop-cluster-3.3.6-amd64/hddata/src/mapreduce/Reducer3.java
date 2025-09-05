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
    private int topK; // Number of top items to emit per region (read from "top.k")

    /**
     * Reads {@code top.k} from the configuration (defaults to 5).
     *
     * @param ctx   Hadoop reducer context
     */
    @Override
    protected void setup(Context ctx)
    {
        topK = ctx.getConfiguration().getInt("top.k", 5);
    }

    /**
     * Sorts items by {@code sumVol} descending and emits Top-K rows for the region.
     *
     * @param region    region name
     * @param values    iterable of lines: "model \t sumVol \t sharePct \t avgPrice \t highShare"
     * @param ctx       Hadoop context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text region, Iterable<Text> values, Context ctx) throws IOException, InterruptedException
    {
        List<String[]> rows = new ArrayList<>();

        // Collect parsed rows for this region
        for (Text t : values)
        {
            String[] p = t.toString().split("\\t");
            if (p.length >= 5)
                rows.add(p);
        }

        // Sort by sumVol descending
        rows.sort((a, b) -> Integer.compare(Integer.parseInt(b[1]), Integer.parseInt(a[1])));

        // Emit Top-K rows
        int k = 0;
        for (String[] r : rows)
        {
            if (k++ >= topK)
                break;

            // Output preserves the original metric order
            ctx.write(region, new Text(r[0] + "\t" + r[1] + "\t" + r[2] + "\t" + r[3] + "\t" + r[4]));
        }
    }
}