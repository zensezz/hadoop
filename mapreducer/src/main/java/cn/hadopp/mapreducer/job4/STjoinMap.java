package cn.hadopp.mapreducer.job4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class STjoinMap extends Mapper<LongWritable,Text,Text,Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(" ");
        String child = split[0];
        String parent = split[1];
        if (!child.equals("child")){
            context.write(new Text(parent),new Text("L"+child));
            context.write(new Text(child),new Text("R"+parent));
        }
    }
}
