package cn.hadopp.mapreducer.content.step2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * item user (评分矩阵)    X    item profile（已转置
 * @Author zz
 * @Date 2018/11/23 002314:43
 * @ClassName Reducer2
 */
public class Reducer2 extends Reducer<Text,Text,Text,Text> {
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
            line = sb.substring(0,sb.length()-1);
        }
        context.write(new Text(key),new Text(line));
    }
}
