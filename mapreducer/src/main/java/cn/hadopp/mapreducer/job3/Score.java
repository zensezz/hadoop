package cn.hadopp.mapreducer.job3;

import cn.hadopp.mapreducer.util.JobUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @deprecated 求平均值
 * @date 2018-11-16
 * @author  zz
 */
public class Score extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        JobUtil.setConf(conf, "Score", conf.get("in"), conf.get("out"), this.getClass());
        JobUtil.setMapper(ScoreMapper.class, Text.class,IntWritable.class, TextInputFormat.class);
        JobUtil.setReducer(ScoreReducer.class,Text.class,IntWritable.class, TextOutputFormat.class);
        return JobUtil.commit();
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Score(),args));
    }
}
