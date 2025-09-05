package it.unisa.diem.hpc.mapreduce.bmw;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

// Reads Aggregator lines: "region \t model \t count|sumVolume|sumPrice|highCount"
// Counter result: (region) -> sumVolume
public class Mapper2 extends Mapper<LongWritable, Text, Text, IntWritable>
{
    private Text outKey = new Text();
    private IntWritable outVal = new IntWritable();

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

        int sumVolume = 0;
        try
        {
            sumVolume = Integer.parseInt(p[1]);
        }
        catch (Exception ex) {}

        outKey.set(region);
        outVal.set(sumVolume);
        ctx.write(outKey, outVal);
    }
}

// Sum volumes per region
public class Reducer2 extends Reducer<Text, IntWritable, Text, IntWritable>
{
    private IntWritable outVal = new IntWritable();

    @Override
    protected void reduce(Text region, Iterable<IntWritable> vals, Context ctx) throws IOException, InterruptedException
    {
        int total = 0;
        for (IntWritable v : vals)
            total += v.get();

        // Output: region \t regionTotalVolume
        outVal.set(total);
        ctx.write(region, outVal);
    }
}
