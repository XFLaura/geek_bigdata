package com.geek.bigdata_learn;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import com.geek.bigdata_learn.FlowBean;

public class FlowCountReduce extends Reducer< Text, FlowBean,Text, FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> flowbean, Context context) throws IOException, InterruptedException {
        //不需处理
        long upFlow = 0;
        long downFlow = 0;

        for(FlowBean bean : flowbean){
            upFlow += bean.getUpFlow();
            downFlow += bean.getDownFlow();
        }

        FlowBean bean = new FlowBean( upFlow, downFlow);
        context.write(key, bean);
    }



}
