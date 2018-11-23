package cn.hadopp.mapreducer.content.step3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

/**
 *  cos<步骤1输入,步骤2输出>
 * @Author zz
 * @Date 2018/11/23 002315:45
 * @ClassName Mapper3
 */
public class Mapper3 extends Mapper<LongWritable,Text,Text,Text>{
    private List<String> cacheList = new ArrayList<String>();
    private DecimalFormat df = new DecimalFormat("0.00");
    /**
     * 在map执行之前会执行这个方法，只会执行一次
     * 通过输入流将全局缓存中的矩阵读入一个java容器中
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        FileReader fr = new FileReader("itemUserScore2");
        BufferedReader br = new BufferedReader(fr);
        //右矩阵
        //key:行号 物品ID
        // value:列号_值,列号_值,列号_值,列号_值,列号_值...    用户ID_分值
        String line = null;
        while ((line=br.readLine())!=null){
            cacheList.add(line);
        }
        fr.close();
        br.close();
    }

    /**
     * @param key 行号	物品ID
     * @param value 行	列_值,列_值,列_值,列_值	用户ID_分值
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] rowma1 = value.toString().split(" ");

        //矩阵行号
        String rowm1 = rowma1[0];
        //列_值
        String[] cloumnvam1 = rowma1[1].split(",");

        //计算左侧矩阵行的空间距离
        double denominator1 = 0;
        for(String s:cloumnvam1){
            String score = s.split("_")[1];
            denominator1 += Double.valueOf(score)*Double.valueOf(score);
        }
        denominator1 = Math.sqrt(denominator1);

        for(String line:cacheList){

            String[] rowma2 = line.split(" ");
            //右侧矩阵line
            //格式: 列 tab 行_值,行_值,行_值,行_值
            String cloumnma2 = rowma2[0];
            String[] rowvam2 = rowma2[1].split(",");

            //计算右侧矩阵行的空间距离
            double denominator2 = 0;
            for(String s:rowvam2){
                String score = s.split("_")[1];
                denominator2 += Double.valueOf(score)*Double.valueOf(score);
            }
            denominator2 = Math.sqrt(denominator2);

            //矩阵两位相乘得到的结果	分子
            double numerator = 0;


            //遍历左侧矩阵一行的每一列

            for(String s:cloumnvam1){
                String c1 = s.split("_")[0];
                String v1 = s.split("_")[1];

                //遍历右侧矩阵一行的每一列
                for(String s2:rowvam2){
                    if(s2.startsWith(c1+"_")){
                        String v2 = s2.split("_")[1];
                        //将两列的值相乘并累加
                        numerator+= Double.valueOf(v1)*Double.valueOf(v2);

                    }
                }
            }

            double cos = numerator/(denominator1*denominator2);
            if(cos == 0){
                continue;
            }
            //cos就是结果矩阵中的某个元素，
            //  行：rowm1
            // 	列：row_matrix2（右侧矩阵已经被转置）
            //输出格式为	key:行	value:列_值
            context.write(new Text(cloumnma2), new Text(rowm1+"_"+df.format(cos)));
        }
    }

}
