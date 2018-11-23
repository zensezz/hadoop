package cn.hadopp.mapreducer.content.step2;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * item user (评分矩阵)    X    item profile（已转置
 * @Author zz
 * @Date 2018/11/23 002314:43
 * @ClassName Mapper2
 *
 */
public class Mapper2 extends Mapper<LongWritable,Text,Text,Text> {

    private List<String> cacheList = new ArrayList<String>();
    private DecimalFormat df = new DecimalFormat("0.00");

    /** 在map执行之前会执行这个方法，只会执行一次
     * 把步骤1中的输出文件读入
     *  通过输入流将全局缓存中的矩阵读入一个java容器中
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        FileReader fr = new FileReader("itemUserScore1");
        BufferedReader br = new BufferedReader(fr);
        // 矩阵
        // key: 行号  物品ID
        // value:列号_值,列号_值,列号_值,列号_值,列号_值...    用户ID_分值，
        String line = null ;
        while ((line=br.readLine()) !=null){
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
        String[] split1 = value.toString().split(" ");
        //矩阵行号
        String row1 = split1[0];
        //列值
        String[] cloum1 = split1[1].split(",");
        cacheList.forEach(i->{
            String[] split2 = i.split(" ");
            //右侧矩阵line
            //格式: 列 tab 行_值,行_值,行_值,行_值
            String row2 = split2[0];
            String[] cloum2 = split2[1].split(",");
            //矩阵两位相乘得到的结果
            double result = 0;

            //遍历左侧矩阵一行的每一列
            for (String c1 : cloum1) {
                String cm1 = c1.split("_")[0];
                String vm1 = c1.split("_")[1];
                //遍历右侧矩阵一行的每一列
                for (String c2 : cloum2) {
                    if (c2.startsWith(cm1+"_")){
                        String vm2 = c2.split("_")[1];
                        // 将两列的值相乘并相加
                        result += Double.valueOf(vm1) * Double.valueOf(vm2);
                    }
                }

                //
                if (result == 0 ){
                    continue;
                }
                //result就是结果矩阵中的某个元素，坐标
                // 行：row_matrix1
                // 列：row_matrix2（右侧矩阵已经被转置）
                try {
                    context.write(new Text(row1),new Text(cloum2+"_"+df.format(result)));
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
