package com.stormjishu.wordCount;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.Random;

//任务的Spout组件：采集数据
public class WordCountSpout extends BaseRichSpout{

    //模拟一些数据
    private String[] data = {"I love Beijing","I love China","Beijing is the capital of China"};

    //如何发送给下一个任务
    private SpoutOutputCollector spoutOutputCollector;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        //spoutOutputController表示：通过它可以把任务发送给下一个任务
        this.spoutOutputCollector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(3000);

        //如何采集数据，需要将采集的数据发给下一个任务
        int random = new Random().nextInt(3);
        String str = data[random];
        System.out.println("采集的数据是："+str);
       spoutOutputCollector.emit(new Values(str));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //定义发送数据的格式
        outputFieldsDeclarer.declare(new Fields("data"));
    }
}
