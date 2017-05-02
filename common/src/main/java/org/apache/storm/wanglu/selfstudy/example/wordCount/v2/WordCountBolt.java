package org.apache.storm.wanglu.selfstudy.example.wordCount.v2;

import com.alibaba.fastjson.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
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
public class WordCountBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(WordCountBolt.class);

    private OutputCollector collector;
    private HashMap<String, Long> counts = null;
    private Integer taskId = null;

    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.counts = new HashMap<String, Long>();
        this.taskId = context.getThisTaskId();
    }

    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long count = this.counts.get(word);
        if(count == null){
            count = 0L;
        }
        count++;
        this.counts.put(word, count);
        this.collector.ack(tuple);
        logger.warn(String.format("WordCountBolt taskId: %d receive tuple: %s messageId is: %s and going to emit it",
                taskId,
                JSONObject.toJSONString(tuple),
                tuple.getMessageId()));
        this.collector.emit(tuple, new Values(word, count));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
