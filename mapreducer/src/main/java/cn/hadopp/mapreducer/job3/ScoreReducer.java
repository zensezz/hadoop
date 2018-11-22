package cn.hadopp.mapreducer.job3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ScoreReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private int avg  =0 ;
    private int count = 0 ;
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        values.forEach(i-> {
                    avg = avg + Integer.parseInt(i.toString());
                    count++;
                });
        context.write(key,new IntWritable(avg/count));
    }
}
