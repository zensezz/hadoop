package cn.hadopp.mapreducer.content.step3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *  cos<步骤1输入,步骤2输出>
 * @Author zz
 * @Date 2018/11/23 002315:45
 * @ClassName Reducer3
 */
public class Reducer3 extends Reducer<Text,Text,Text,Text> {
    /**
     * @param key 行 物品ID
     * @param values 列_值	用户ID_分值
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        values.forEach(i->sb.append(i).append(","));
        String line = null ;
        if (sb.toString().endsWith(",")){
            line=sb.substring(0,sb.length()-1);
        }
        context.write(key,new Text(line));
    }
}
