package org.apache.storm.wanglu.selfstudy.example.window;

import com.alibaba.fastjson.JSONObject;

import org.apache.storm.wanglu.selfstudy.util.Assert;
import org.apache.storm.wanglu.selfstudy.util.JStormHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseWindowedBolt;
import backtype.storm.topology.base.BaseWindowedBolt.Count;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.windowing.TupleWindow;


/**
 * Created by wanglu on 2017/4/28.
 */
public class SlidingWindowTopology {

    private static final Logger LOG = LoggerFactory.getLogger(SlidingWindowTopology.class);

    /*
     * Computes tumbling window average
     */
    private static class TumblingWindowAvgBolt extends BaseWindowedBolt {
        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(TupleWindow inputWindow) {
            int sum = 0;
            List<Tuple> tuplesInWindow = inputWindow.get();
            LOG.error("Events in current window: " + tuplesInWindow.size());
            if (tuplesInWindow.size() > 0) {
                /*
                 * Since this is a tumbling window calculation, we use all the
                 * tuples in the window to compute the avg.
                 */
                for (Tuple tuple : tuplesInWindow) {
                    sum += Integer.parseInt(tuple.getValue(0).toString());
                }
                LOG.error(String.format("TumblingWindowAvgBolt gong2to emit inputWindow: %s and sum is: %d",
                        JSONObject.toJSONString(inputWindow.get()), sum));
                collector.emit(new Values(sum / tuplesInWindow.size()));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("avg"));
        }
    }

    static boolean isLocal = true;
    static Config conf = JStormHelper.getConfig(null);

    public static void test() {

        String[] className = Thread.currentThread().getStackTrace()[1].getClassName().split("\\.");
        String topologyName = className[className.length - 1];

        try {
            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout("integer", new RandomIntegerSpout(), 3);
            builder.setBolt("slidingsum", new SlidingWindowSumBolt().withWindow(new Count(30), new Count(10)), 1)
                    .shuffleGrouping("integer");
            builder.setBolt("tumblingavg", new TumblingWindowAvgBolt().withWindow(new Count(3), new Count(1)), 1)
                    .shuffleGrouping("slidingsum");
            builder.setBolt("printer", new PrinterBolt(), 1).shuffleGrouping("tumblingavg");

            conf.setDebug(true);

            JStormHelper.runTopology(builder.createTopology(), topologyName, conf, 10,
                    new JStormHelper.CheckAckedFail(conf), isLocal);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            Assert.fail("Failed to submit topology");
        }
    }

    public static void main(String[] args) throws Exception {
        isLocal = true;
        conf = JStormHelper.getConfig(args);
        test();
    }

}