package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

// Mapper for (region\tmodel) -> "1|volume|price|isHigh"
public class Mapper1 extends Mapper<LongWritable, Text, Text, Text>
{
    private Text outKey = new Text();
    private Text outVal = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException
    {
        // Splitting CSV by commas
        String line = value.toString().trim();
        if (line.isEmpty())
            return;

        String[] f = line.split(",", -1);
        // Header should start with "Model"
        if (f.length < 11 || "Model".equalsIgnoreCase(f[0]))
            return;

        // Extract fields (by index from the dataset)
        String model   = f[0].trim().toLowerCase();
        String region  = f[2].trim().toLowerCase();
        String prices  = f[8].trim();
        String volumes = f[9].trim();
        String cls     = f[10].trim(); // "High" or "Low" sales classification

        if (region.isEmpty() || model.isEmpty())
            return;

        int volume = 0;
        int price  = 0;
        try
        {
            volume = Integer.parseInt(volumes);
        }
        catch (Exception ignored) {}
        try
        {
            price  = Integer.parseInt(prices);
        }
        catch (Exception ignored) {}

        int isHigh = "high".equalsIgnoreCase(cls) ? 1 : 0;

        // Key is "region\tmodel"
        outKey.set(region + "\t" + model);
        // Value is a compact tagged payload: count|sumVolume|sumPrice|highCount
        outVal.set("1|" + volume + "|" + price + "|" + isHigh);
        ctx.write(outKey, outVal);
    }
}

// Combiner to reduce shuffle size (sums the four integers)
public class Combiner1 extends Reducer<Text, Text, Text, Text> {
    private Text outVal = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> vals, Context ctx) throws IOException, InterruptedException
    {
        int c = 0, vol = 0, prc = 0, hi = 0;
        for (Text t : vals)
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

// Reducer to make final aggregation per (region,model)
public class Reducer1 extends Reducer<Text, Text, Text, Text>
{
    private Text outVal = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> vals, Context ctx) throws IOException, InterruptedException
    {
        int c=0, vol=0, prc=0, hi=0;
        for (Text t : vals)
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

        // Output format for Counter:
        // key = "region\tmodel", value = "count|sumVolume|sumPrice|highCount"
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