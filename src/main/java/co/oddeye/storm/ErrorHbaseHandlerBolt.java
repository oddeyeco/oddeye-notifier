/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm;

import co.oddeye.core.OddeeyMetric;
import co.oddeye.core.OddeeyMetricMeta;
import co.oddeye.core.OddeeysSpecialMetric;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class ErrorHbaseHandlerBolt extends BaseRichBolt {

    protected OutputCollector collector;

//    private static final Logger LOGGER = Logger.getLogger(KafkaOddeyeMsgToTSDBBolt.class);
    public static final Logger LOGGER = LoggerFactory.getLogger(ErrorHbaseHandlerBolt.class);
    private final Properties conf;
    private final String topic;        

    /**
     *
     * @param config
     * @param s_topic
     */
    public ErrorHbaseHandlerBolt(Properties config, String s_topic) {
        this.conf = config;
        this.topic = s_topic;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector oc) {
        collector = oc;
    }

    @Override
    public void execute(Tuple tuple) {
//        LOGGER.warn("getFields count = " + tuple.getFields().size());
        OddeeyMetric metric = (OddeeyMetric) tuple.getValueByField("metric");
        OddeeyMetricMeta mtrsc = (OddeeyMetricMeta) tuple.getValueByField("mtrsc");
        if (mtrsc.getErrorState().getState() != 1) {
            LOGGER.info("Ready write to hbase Name:" + metric.getName() + "Host:" + metric.getTags().get("host"));
        }

//        OddeeyMetric metric = (OddeeyMetric) tuple.getValueByField("metric");
        collector.ack(tuple);
    }

}
