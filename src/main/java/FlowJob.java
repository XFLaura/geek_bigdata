package com.geek.bigdata_learn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FlowJob  {


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"G20210735010269_first");


        //2.指定mr运行的类
        job.setJarByClass(FlowJob.class);

        //3.指定mr程序的m，r类
        job.setMapperClass(com.geek.bigdata_learn.FlowCountMapper.class);
        job.setReducerClass(com.geek.bigdata_learn.FlowCountReduce.class);

        //4指定map输出kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(com.geek.bigdata_learn.FlowBean.class);

        //5.指定ruduce输出的kv类型，也就是mr的最终输出结果
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(com.geek.bigdata_learn.FlowBean.class);

        //6.指定mr程序的输出路径
        //设置MR程序的文件输入目录和最终结果的输出目录
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        //文件系统中如果存在输出路径，就删除文件系统中输出路径
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean result = job.waitForCompletion(true);

        System.out.println(result ? "success" : "failed");

    }
}
