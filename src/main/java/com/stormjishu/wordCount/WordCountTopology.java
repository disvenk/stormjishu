package com.stormjishu.wordCount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalCluster$_uploadNewCredentials;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.Currency;

//主程序
public class WordCountTopology {
    public static void main(String[] args) {
        //创建一个任务，并且指定任务的spout组件和多个bolt组件
        TopologyBuilder builder = new TopologyBuilder();
        //指定spout任务用于采集数据
        builder.setSpout("myspout",new WordCountSpout());

        //指定任务的bolt用于分词，计数;shuffleGrouping用于关联spout
        builder.setBolt("mysplit",new WorkCountSplitBolt()).shuffleGrouping("myspout");

        //fieldsGrouping采用字段分组策略，分组字段为word
        builder.setBolt("mycount",new WorkCountTotalBolt()).fieldsGrouping("mysplit",new Fields("word"));

        //创建任务
        StormTopology job = builder.createTopology();

        //直接在idea中运行
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("mydemo",new Config(),job);
    }
}
