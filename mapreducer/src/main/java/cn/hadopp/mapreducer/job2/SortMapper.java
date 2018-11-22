package cn.hadopp.mapreducer.job2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable,Text,IntWritable,IntWritable> {
    private static IntWritable data=new IntWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        data.set(Integer.parseInt(line));
        context.write(data, new IntWritable(1));
    }
}
