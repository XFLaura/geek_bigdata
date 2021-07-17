package com.geek.bigdata_learn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text,Text, com.geek.bigdata_learn.FlowBean> {

    private FlowBean flowBean = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split("\t");
        String phone = split[1];
        flowBean.setUpFlow(Long.parseLong(split[8]));
        flowBean.setDownFlow(Long.parseLong(split[9]));
        context.write(new Text(phone),flowBean);
    }
}

