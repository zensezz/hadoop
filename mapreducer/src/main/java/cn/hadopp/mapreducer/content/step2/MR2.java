package cn.hadopp.mapreducer.content.step2;

import cn.hadopp.mapreducer.util.JobUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * item user (评分矩阵)    X    item profile（已转置
 * @Author zz
 * @Date 2018/11/23 002314:43
 * @ClassName MR2
 */
public class MR2 extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        JobUtil.setConf(conf, "step2", conf.get("in"), conf.get("out"), this.getClass());
        JobUtil.setCatch(conf.get("catch"),"#itemUserScore1");
        JobUtil.setMapper(Mapper2.class, Text.class,Text.class, TextInputFormat.class);
        JobUtil.setReducer(Reducer2.class, Text.class, Text.class, TextOutputFormat.class);
        return JobUtil.commit();
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MR2(),args));
    }
}
