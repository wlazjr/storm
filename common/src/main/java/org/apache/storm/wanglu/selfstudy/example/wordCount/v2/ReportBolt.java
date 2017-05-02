package org.apache.storm.wanglu.selfstudy.example.wordCount.v2;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * Created by wanglu on 2017/5/1.
 */
public class ReportBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(ReportBolt.class);

    private static ConcurrentHashMap<String, Long> counts = null;
    private OutputCollector collector;
    private Integer taskId = null;

    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        this.counts = new ConcurrentHashMap<String, Long>();
        this.collector = collector;
        this.taskId = context.getThisTaskId();

    }

    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");
        this.counts.put(word, count);
        this.collector.ack(tuple);
        logger.warn(String.format(String.format("ReportBolt taskId: %d receive tuple: %s and messageId is: %s",
                taskId,
                JSONObject.toJSONString(tuple),
                tuple.getMessageId())));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // this bolt does not emit anything
    }

    @Override
    public void cleanup() {
        System.out.println("--- FINAL COUNTS ---");
        List<String> keys = new ArrayList<String>();
        keys.addAll(this.counts.keySet());
        Collections.sort(keys);
        logger.warn("ReportBolt print final result is: " + JSONObject.toJSONString(counts, SerializerFeature.WriteMapNullValue));
        logger.warn("FINISH ReportBolt");
    }
}
