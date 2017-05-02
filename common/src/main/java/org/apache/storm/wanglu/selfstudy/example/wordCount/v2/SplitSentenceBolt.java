package org.apache.storm.wanglu.selfstudy.example.wordCount.v2;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Created by wanglu on 2017/5/1.
 */
public class SplitSentenceBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(SplitSentenceBolt.class);
    private OutputCollector collector;
    private Integer taskId = null;

    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.taskId = context.getThisTaskId();
    }

    public void execute(Tuple tuple) {
        String sentence = tuple.getStringByField("sentence");
        String[] words = sentence.split(" ");
        for(String word : words){
            this.collector.emit(tuple, new Values(word));
        }
        this.collector.ack(tuple);

        logger.warn(String.format("SplitSentenceBolt taskid: %d acked tuple: %s and messageId is: %s",
                taskId,
                JSONObject.toJSONString(tuple, SerializerFeature.WriteMapNullValue),
                tuple.getMessageId()));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
