/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.shade.org.yaml.snakeyaml.Yaml;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

/**
 *
 * @author vahan
 */
public class NotifierTopology {

    private static String topologyname;

    public static void main(String[] args) {
        String Filename = args[0];
        Config topologyconf = new Config();
        try {
            Yaml yaml = new Yaml();
            java.util.Map rs = (java.util.Map) yaml.load(new InputStreamReader(new FileInputStream(Filename)));
            topologyconf.putAll(rs);
        } catch (FileNotFoundException e) {
            System.out.println(e);
        }

        java.util.Map<String, Object> kafkaconf = (java.util.Map<String, Object>) topologyconf.get("Kafka");
        java.util.Map<String, Object> kafkasemaphoreconf = (java.util.Map<String, Object>) topologyconf.get("KafkaSemaphore");
        java.util.Map<String, Object> tconf = (java.util.Map<String, Object>) topologyconf.get("Topology");

        TopologyBuilder builder = new TopologyBuilder();
//        HBaseBolt
// zookeeper hosts for the Kafka cluster
        BrokerHosts zkHosts = new ZkHosts(String.valueOf(kafkaconf.get("zkHosts")));
// Create the KafkaSpout configuration
// Second argument is the topic name
// Third argument is the ZooKeeper root for Kafka
// Fourth argument is consumer group id

        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts,
                String.valueOf(kafkaconf.get("topic")), String.valueOf(kafkaconf.get("zkRoot")), String.valueOf(kafkaconf.get("zkKey")));

        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        builder.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), Integer.parseInt(String.valueOf(tconf.get("SpoutParallelism_hint"))));     
        builder.setSpout("TimerSpout2x", new TimerSpout2x(), 1);
        
        // Semaphore bolt
        
        BrokerHosts zkSemaphoreHosts = new ZkHosts(String.valueOf(kafkasemaphoreconf.get("zkHosts")));
        SpoutConfig kafkaSemaphoreConfig = new SpoutConfig(zkSemaphoreHosts,
                String.valueOf(kafkasemaphoreconf.get("topic")), String.valueOf(kafkasemaphoreconf.get("zkRoot")), String.valueOf(kafkasemaphoreconf.get("zkKey")));
        kafkaSemaphoreConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        builder.setSpout("kafkaSemaphoreSpot", new KafkaSpout(kafkaSemaphoreConfig), Integer.parseInt(String.valueOf(tconf.get("SpoutSemaphoreParallelism_hint"))));         

        java.util.Map<String, Object> TSDBconfig = (java.util.Map<String, Object>) topologyconf.get("Tsdb");
        java.util.Map<String, Object> Mailconfig = (java.util.Map<String, Object>) topologyconf.get("mail");

        builder.setBolt("ParseMetricBolt",
                new ParseMetricErrorBolt(TSDBconfig), Integer.parseInt(String.valueOf(tconf.get("ParseMetricBoltParallelism_hint"))))
                .shuffleGrouping("KafkaSpout");

        builder.setBolt("MetricErrorToHbase",
                new MetricErrorToHbase(TSDBconfig), Integer.parseInt(String.valueOf(tconf.get("MetricErrorToHbaseParallelism_hint"))))
                .shuffleGrouping("ParseMetricBolt");        

        builder.setBolt("SendNotifierBolt",
                new SendNotifierBolt(TSDBconfig, Mailconfig), Integer.parseInt(String.valueOf(tconf.get("SendNotifierBoltParallelism_hint"))))
                .customGrouping("ParseMetricBolt", new MetaByUserGrouper())
                .allGrouping("TimerSpout2x")
                .allGrouping("kafkaSemaphoreSpot");                
        
        
        Config conf = new Config();
        conf.setNumWorkers(Integer.parseInt(String.valueOf(tconf.get("NumWorkers"))));
//        conf.put(Config.TOPOLOGY_DEBUG, true);
        conf.setMaxSpoutPending(Integer.parseInt(String.valueOf(tconf.get("topology.max.spout.pending"))));
        conf.setDebug(Boolean.getBoolean(String.valueOf(tconf.get("Debug"))));
        conf.setMessageTimeoutSecs(Integer.parseInt(String.valueOf(tconf.get("topology.message.timeout.secs"))));
        try {
// This statement submit the topology on remote cluster. // args[0] = name of topology StormSubmitter.
            topologyname = String.valueOf(tconf.get("topologi.display.name"));
            StormSubmitter.submitTopology(topologyname, conf, builder.createTopology());
        } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException alreadyAliveException) {
            System.out.println(alreadyAliveException);
        }
    }
}
