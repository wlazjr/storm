package org.apache.storm.wanglu.selfstudy.example.wordCount.v2;

import com.alibaba.fastjson.JSONObject;

import org.apache.storm.wanglu.selfstudy.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * Created by wanglu on 2017/5/1.
 */
public class SentenceSpout extends BaseRichSpout {
    private static final Logger logger = LoggerFactory.getLogger(SentenceSpout.class);

    private ConcurrentHashMap<UUID, Values> pending;
    private SpoutOutputCollector collector;
    private String[] sentences = {
            "my dog has fleas",
            "i like cold beverages",
            "the dog ate my homework",
            "don't have a cow man",
            "i don't think i like fleas"
    };
    private AtomicInteger index = new AtomicInteger(0);

    private Integer taskId = null;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

    public void open(Map config, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
        this.pending = new ConcurrentHashMap<UUID, Values>();
        this.taskId = context.getThisTaskId();
    }

    public void nextTuple() {
        Values values = new Values(sentences[index.getAndIncrement()]);
        UUID msgId = UUID.randomUUID();
        this.pending.put(msgId, values);
        this.collector.emit(values, msgId);
        if (index.get() >= sentences.length) {
            index = new AtomicInteger(0);
        }
        logger.warn(String.format("SentenceSpout with taskId: %d emit msgId: %s and tuple is: %s",
                taskId,
                msgId,
                JSONObject.toJSON(values)));
        Utils.waitForMillis(100);
    }

    public void ack(Object msgId) {
        this.pending.remove(msgId);
        logger.warn(String.format("SentenceSpout taskId: %d receive msgId: %s and remove it from the pendingmap",
                taskId,
                JSONObject.toJSONString(msgId)));
    }

    public void fail(Object msgId) {
        logger.error(String.format("SentenceSpout taskid: %d receive msgId: %s and remove it from the pendingmap",
                taskId,
                JSONObject.toJSONString(msgId)));
        this.collector.emit(this.pending.get(msgId), msgId);
    }
}

