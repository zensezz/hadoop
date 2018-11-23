package cn.hadopp.mapreducer.content.step1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *  将item profile转置
 *
 * @Author zz
 * @Date 2018/11/23  14:02
 * @ClassName Reducer1
 */
public class Reducer1 extends Reducer<Text,Text,Text,Text> {
    /**
     *
     * @param key 列号	 用户ID
     * @param values 行号_值,行号_值,行号_值,行号_值...	物品ID_分值
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        //text:行号_值		物品ID_分值
        values.forEach(i->{
            sb.append(i).append(",");
        });
        String line = null ;
        if (sb.toString().endsWith(",")){
            line = sb.substring(0,sb.length()-1);
        }
        context.write(key,new Text(line));
    }
}
