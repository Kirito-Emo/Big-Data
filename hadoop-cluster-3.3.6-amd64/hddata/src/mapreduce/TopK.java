package it.unisa.diem.hpc.mapreduce.bmw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.*;
import java.util.*;

// Loads region totals (Counter output) in setup(), then processes Aggregator lines
// Results: key=region, value="model \t sumVolume \t sharePct \t avgPrice \t highShare"
public class Mapper3 extends Mapper<LongWritable, Text, Text, Text>
{
    private final Map<String,Integer> regionTotals = new HashMap<>();
    private Text outKey = new Text();
    private Text outVal = new Text();

    @Override
    protected void setup(Context ctx) throws IOException
    {
        Configuration conf = ctx.getConfiguration();
        String totalsPath = conf.get("step2.totals.path");
        if (totalsPath == null)
            return;

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
                while ((line = br.readLine()) != null)
                {
                    String[] kv = line.split("\\t");
                    if (kv.length >= 2)
                    {
                        try
                        {
                            regionTotals.put(kv[0], Integer.parseInt(kv[1]));
                        }
                        catch (Exception ex) {}
                    }
                }
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException
    {
        // Input is Aggregator output: region \t model \t count|sumVolume|sumPrice|highCount
        String[] kv = value.toString().split("\\t");
        if (kv.length < 3)
            return;

        String region = kv[0].trim();
        String model  = kv[1].trim();
        String[] p = kv[2].split("\\|");
        if (p.length < 4)
            return;

        int count = 0, sumVol = 0, sumPrice = 0, high = 0;
        try
        {
            count = Integer.parseInt(p[0]);
        }
        catch (Exception ex) {}

        try
        {
            sumVol = Integer.parseInt(p[1]);
        }
        catch (Exception ex) {}

        try
        {
            sumPrice = Integer.parseInt(p[2]);
        }
        catch (Exception ex) {}

        try
        {
            high = Integer.parseInt(p[3]);
        }
        catch (Exception ex) {}

        Integer tot = regionTotals.get(region);
        double share = (tot != null && tot > 0) ? (100.0 * sumVol / tot) : 0.0;
        double avgPrice = (count > 0) ? ((double) sumPrice / count) : 0.0;
        double highShare = (count > 0) ? (100.0 * high / count) : 0.0;

        outKey.set(region);
        outVal.set(
                model
                + "\t" + sumVol
                + "\t" + String.format(java.util.Locale.US, "%.4f", share)
                + "\t" + String.format(java.util.Locale.US, "%.2f", avgPrice)
                + "\t" + String.format(java.util.Locale.US, "%.2f", highShare)
        );
        ctx.write(outKey, outVal);
    }
}

// Sort items by sumVolume and emit Top-K per region
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
        // Parse to [model, sumVol, sharePct, avgPrice, highShare]
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

            // Output format:
            // region \t model \t sumVol \t sharePct \t avgPrice \t highShare
            ctx.write(region, new Text(r[0] + "\t" + r[1] + "\t" + r[2] + "\t" + r[3] + "\t" + r[4]));
        }
    }
}