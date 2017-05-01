package org.apache.storm.wanglu.selfstudy.example.window;

import com.alibaba.fastjson.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * Created by wanglu on 2017/4/28.
 */
public class PrinterBolt extends BaseBasicBolt {
    private static final Logger logger = LoggerFactory.getLogger(BaseBasicBolt.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        logger.error("PrinterBoltReuslt going to print: " + JSONObject.toJSONString(tuple));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

}