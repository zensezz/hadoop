package cn.hadopp.mapreducer.job2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
    private static IntWritable linenum = new IntWritable(1);
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        values.forEach(i->{
            try {
                context.write(linenum, key);
                linenum = new IntWritable(linenum.get()+1);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
    }
}
