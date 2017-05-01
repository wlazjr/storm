package org.apache.storm.wanglu.selfstudy.example.window;

import com.alibaba.fastjson.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseWindowedBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.windowing.TupleWindow;

/**
 * Created by wanglu on 2017/4/28.
 */
public class SlidingWindowSumBolt extends BaseWindowedBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SlidingWindowSumBolt.class);

    private int sum = 0;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        /*
         * The inputWindow gives a view of (a) all the events in the window (b)
         * events that expired since last activation of the window (c) events
         * that newly arrived since last activation of the window
         */
        List<Tuple> tuplesInWindow = inputWindow.get();
        List<Tuple> newTuples = inputWindow.getNew();
        List<Tuple> expiredTuples = inputWindow.getExpired();

        LOG.error("Events in current window: " + tuplesInWindow.size());
        /*
         * Instead of iterating over all the tuples in the window to compute the
         * sum, the values for the new events are added and old events are
         * subtracted. Similar optimizations might be possible in other
         * windowing computations.
         */
        for (Tuple tuple : newTuples) {
            sum += Integer.parseInt(tuple.getValue(0).toString());
        }
        for (Tuple tuple : expiredTuples) {
            sum -= Integer.parseInt(tuple.getValue(0).toString());
        }
        LOG.error("going2print inputWindow: " + JSONObject.toJSONString(inputWindow.get()));

        collector.emit(new Values(sum));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sum"));
    }
}
