package org.apache.storm.wanglu.selfstudy.example.window;

import org.apache.storm.wanglu.selfstudy.util.Assert;
import org.apache.storm.wanglu.selfstudy.util.JStormHelper;

import java.util.concurrent.TimeUnit;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseWindowedBolt;
import backtype.storm.topology.base.BaseWindowedBolt.Duration;

/**
 * Created by wanglu on 2017/4/28.
 */
public class SlidingTupleTsTopology {
    static boolean isLocal = true;
    static Config conf = JStormHelper.getConfig(null);

    public static void test() {

        String[] className = Thread.currentThread().getStackTrace()[1].getClassName().split("\\.");
        String topologyName = className[className.length - 1];

        try {
            TopologyBuilder builder = new TopologyBuilder();
            BaseWindowedBolt bolt = new SlidingWindowSumBolt()
                    .withWindow(new Duration(5, TimeUnit.SECONDS), new Duration(3, TimeUnit.SECONDS))
                    .withTimestampField("ts").withLag(new Duration(5, TimeUnit.SECONDS));
            builder.setSpout("integer", new RandomIntegerSpout(), 1);
            builder.setBolt("slidingsum", bolt, 1).shuffleGrouping("integer");
            builder.setBolt("printer", new PrinterBolt(), 1).shuffleGrouping("slidingsum");

            conf.setDebug(true);

            JStormHelper.runTopology(builder.createTopology(), topologyName, conf, 60,
                    new JStormHelper.CheckAckedFail(conf), isLocal);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            Assert.fail("Failed");
        }
    }

    public static void main(String[] args) throws Exception {
        isLocal = false;
        conf = JStormHelper.getConfig(args);
        test();
    }
}