package edu.nwnu.ququzone.storm.demo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import edu.nwnu.ququzone.storm.demo.bolt.ExclamationBolt;
import edu.nwnu.ququzone.storm.demo.spout.WordSpout;

/**
 * Demo topology.
 *
 * @author Yang XuePing
 */
public class DemoTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("words", new WordSpout(), 10);
        builder.setBolt("exclaim1", new ExclamationBolt(), 2).shuffleGrouping("words");
        builder.setBolt("exclaim2", new ExclamationBolt(), 3).shuffleGrouping("exclaim1");

        Config conf = new Config();

        if (args != null && args.length > 0) {
            conf.setNumWorkers(20);
            conf.setMaxSpoutPending(50);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            conf.setDebug(true);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}
