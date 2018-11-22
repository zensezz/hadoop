package cn.hadopp.mapreducer.job1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DedupReducer extends Reducer<Text, Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        values.forEach(i -> {
            try {
                context.write(key, i);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
    }
}
