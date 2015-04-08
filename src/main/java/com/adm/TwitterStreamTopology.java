package com.adm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class TwitterStreamTopology {
    public static void main(String[] args) {

        // Twitter Keys
        String consumerKey = args[0];
        String consumerSecret = args[1];
        String accessTokenKey = args[2];
        String accessTokenSecret = args[3];

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("Streams", new StreamSpout(consumerKey, consumerSecret,
                accessTokenKey, accessTokenSecret));
//        topologyBuilder.setBolt("HashTags", new HashTagBolt(), 2).allGrouping("Streams");
        topologyBuilder.setBolt("Users", new UserBolt(), 2).allGrouping("Streams");

        Config conf = new Config();
        conf.setNumWorkers(2);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, topologyBuilder.createTopology());

        Utils.sleep(10000);
        cluster.shutdown();
    }
}
