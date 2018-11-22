package cn.hadopp.mapreducer.job2;

import cn.hadopp.mapreducer.util.JobUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @deprecated 排序
 * @date 2018-11-16
 * @author  zz
 */
public class Sort extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        JobUtil.setConf(conf, "Sort", conf.get("in"), conf.get("out"), this.getClass());
        JobUtil.setMapper(SortMapper.class, IntWritable.class,IntWritable.class, TextInputFormat.class);
        JobUtil.setReducer(SortReducer.class,IntWritable.class,IntWritable.class, TextOutputFormat.class);
        return JobUtil.commit();
    }
    public static void main(String[] args) throws Exception{
        System.exit(ToolRunner.run(new Sort(),args));
    }
}
