/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm;

import co.oddeye.core.OddeeyMetricMeta;
import co.oddeye.core.OddeeyMetricMetaList;
import co.oddeye.core.globalFunctions;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.ArrayUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.hbase.async.PutRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class MetricErrorToHbase extends BaseRichBolt {

    protected OutputCollector collector;
    public static final Logger LOGGER = LoggerFactory.getLogger(MetricErrorToHbase.class);
    private JsonParser parser = null;
    private final Map conf;
    private Config openTsdbConfig;
    private org.hbase.async.Config clientconf;
    private byte[] metatable;
    private OddeeyMetricMetaList mtrscList;
    private byte[] errorshistorytable;
    private byte[][] qualifiers;
    private byte[][] values;

    public MetricErrorToHbase(java.util.Map config) {
        this.conf = config;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("metric"));
    }

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        try {
            LOGGER.warn("DoPrepare ParseMetricErrorBolt");
            collector = oc;
            parser = new JsonParser();
            String quorum = String.valueOf(conf.get("zkHosts"));
            LOGGER.error("quorum: " + quorum);
            openTsdbConfig = new net.opentsdb.utils.Config(true);
            openTsdbConfig.overrideConfig("tsd.core.auto_create_metrics", String.valueOf(conf.get("tsd.core.auto_create_metrics")));
            openTsdbConfig.overrideConfig("tsd.storage.enable_compaction", String.valueOf(conf.get("tsd.storage.enable_compaction")));
            openTsdbConfig.overrideConfig("tsd.storage.hbase.data_table", String.valueOf(conf.get("tsd.storage.hbase.data_table")));
            openTsdbConfig.overrideConfig("tsd.storage.hbase.uid_table", String.valueOf(conf.get("tsd.storage.hbase.uid_table")));

            clientconf = new org.hbase.async.Config();
            clientconf.overrideConfig("hbase.zookeeper.quorum", quorum);
            clientconf.overrideConfig("hbase.rpcs.batch.size", "2048");
            TSDB tsdb = globalFunctions.getSecindarytsdb(openTsdbConfig, clientconf);
            LOGGER.error("tsdb: " + tsdb);
            this.metatable = String.valueOf(conf.get("metatable")).getBytes();
            this.errorshistorytable = String.valueOf(conf.get("errorshistorytable")).getBytes();
            mtrscList = new OddeeyMetricMetaList();
        } catch (IOException ex) {
            LOGGER.error("ERROR: " + globalFunctions.stackTrace(ex));
        }
    }

    @Override
    public void execute(Tuple input) {
//        String msg = input.getString(0);        
        try {
            OddeeyMetricMeta metric = (OddeeyMetricMeta) input.getValueByField("metric");

            byte[] key = ArrayUtils.addAll(globalFunctions.getDayKey(metric.getErrorState().getTime()), metric.getUUIDKey());
            //+" timekey:"+Hex.encodeHexString(globalFunctions.getNoDayKey(metric.getErrorState().getTime()))
            qualifiers = new byte[2][];
            values = new byte[2][];

            qualifiers[0] = "lastlevel".getBytes();
            qualifiers[1] = globalFunctions.getNoDayKey(metric.getErrorState().getTime());
            values[0] =  ByteBuffer.allocate(1).put((byte) metric.getErrorState().getLevel()).array();
            values[1] = ByteBuffer.allocate(1).put((byte) metric.getErrorState().getLevel()).array();
            LOGGER.warn("metric:" + metric.getName() + " Host:" + metric.getTags().get("host").getValue() + " Err:" + metric.getErrorState().getLevel() + " state:" + metric.getErrorState().getState() + " time:" + metric.getErrorState().getTime() + " daykey:" + Hex.encodeHexString(key));

            PutRequest put = new PutRequest(errorshistorytable, metric.getKey(), "d".getBytes(),qualifiers ,values);
            globalFunctions.getSecindaryclient(clientconf).put(put);

        } catch (Exception ex) {
            LOGGER.error("ERROR: " + globalFunctions.stackTrace(ex));
        }
        this.collector.ack(input);

    }

}
