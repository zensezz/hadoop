package cn.hadopp.mapreducer.job4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class STjoinReducer extends Reducer<Text,Text,Text,Text>{
    private int count = 0 ;
    private String rela = "" ;
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if (count == 0) {
            context.write(new Text("grandchild"), new Text("grandchild"));
            count++;
        }
        List grandchild = new ArrayList();
        List grandparent = new ArrayList();
        values.forEach(i -> {
            rela = i.toString().substring(0, 1);
            if ("L".equals(rela)) {
                grandchild.add(i.toString().substring(1));
            }
            if ("R".equals(rela)) {
                grandparent.add(i.toString().substring(1));
            }
            grandchild.forEach(m->{
                grandparent.forEach(n -> {
                    try {
                        context.write(new Text(m.toString()), new Text(n.toString()));
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                );
            });
        });

    }
}
