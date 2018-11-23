package cn.hadopp.mapreducer.content.step1;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *  将item profile转置
 * @Author zz
 * @Date 2018/11/23 14:02
 * @ClassName Mapper1
 */
public class Mapper1 extends Mapper<LongWritable,Text,Text,Text> {
    /**
     * @param key 1
     * @param value 1	1_0,2_3,3_-1,4_2,5_-3
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(" ");
        // 矩阵行号 物品ID
        String itemID  = split[0];
        // 列值	 用户ID_分值
        String[] lines = split[1].split(",");
        for (String line : lines) {
            String userID = line.split("_")[0];
            String score = line.split("_")[1];
            //key:列号	 用户ID	value:行号_值	 物品ID_分值
            context.write(new Text(itemID), new Text(itemID+"_"+score));
        }
    }
}
