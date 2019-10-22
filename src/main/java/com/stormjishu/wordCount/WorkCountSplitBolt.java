package com.stormjishu.wordCount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;
import java.util.Random;

//第一个Spolt组件：用于拆分单词
public class WorkCountSplitBolt extends BaseRichBolt{

    private OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        //同理
        this.outputCollector = outputCollector;
    }



    @Override
    public void execute(Tuple tuple) {
        //从上一个任务中接收的数据如何处理
        //必须跟定义的格式一样
        String data = tuple.getStringByField("data");
        //分词
        String[] words = data.split(" ");
        for (String word : words){
            this.outputCollector.emit(new Values(word,1));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //发送给下一个任务的数据格式
        outputFieldsDeclarer.declare(new Fields("word","count"));
    }
}
