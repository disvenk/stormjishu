package com.stormjishu.wordCount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import sun.java2d.SurfaceDataProxy;

import java.util.HashMap;
import java.util.Map;

//任务的第二个Bolt组件：单词的计数
public class WorkCountTotalBolt extends BaseRichBolt{

    //定义一个集合，来保存最终的结果
    private Map<String,Integer> result = new HashMap<>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {
        //统计每个单词的总频率
        String word = tuple.getStringByField("word");
        int count = tuple.getIntegerByField("count");
        //如果实现已经包含了，那么要取出来做叠加后再放回去
        if(result.containsKey(word)){
            int total = result.get(word);
            result.put(word,total+ count);
        }else {
            //第一次出现
            result.put(word,count);
        }

        //输出到天猫大屏幕
        System.out.println("统计的结果是"+result);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
