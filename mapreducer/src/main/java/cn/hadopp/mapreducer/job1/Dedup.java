package cn.hadopp.mapreducer.job1;

import cn.hadopp.mapreducer.util.JobUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @deprecated 去重
 * @date 2018-11-16
 * @author  zz
 */
public class Dedup extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        JobUtil.setConf(conf, "Dedup", conf.get("in"), conf.get("out"), this.getClass());
        JobUtil.setMapper(DedupMapper.class, Text.class,Text.class, TextInputFormat.class);
        JobUtil.setReducer(DedupReducer.class, Text.class, Text.class, TextOutputFormat.class);
        return JobUtil.commit();
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Dedup(),args));
    }
}
