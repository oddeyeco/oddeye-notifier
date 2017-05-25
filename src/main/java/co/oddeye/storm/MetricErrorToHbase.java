/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm;

import co.oddeye.core.OddeeyMetricMeta;
import co.oddeye.core.globalFunctions;
import co.oddeye.storm.core.StormUser;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.logging.Level;
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
import org.hbase.async.DeleteRequest;
import org.hbase.async.PutRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author vahan
 */
public class MetricErrorToHbase extends BaseRichBolt {

    protected OutputCollector collector;
    public static final Logger LOGGER = LoggerFactory.getLogger(MetricErrorToHbase.class);
    private final Map conf;
    private final int qualifiersCount = 8;
    private Config openTsdbConfig;
    private org.hbase.async.Config clientconf;
    private byte[] errorshistorytable;
    private byte[] errorslasttable;
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
            if (tsdb == null) {
                LOGGER.error("tsdb: " + tsdb);
            }
            this.errorshistorytable = String.valueOf(conf.get("errorshistorytable")).getBytes();
            this.errorslasttable = String.valueOf(conf.get("errorslasttable")).getBytes();
        } catch (IOException ex) {
            LOGGER.error("ERROR: " + globalFunctions.stackTrace(ex));
        }
    }

    @Override
    public void execute(Tuple tuple) {
        this.collector.ack(tuple);
        if (tuple.getSourceComponent().equals("ParseMetricBolt")) {
            try {
                OddeeyMetricMeta metricMeta = (OddeeyMetricMeta) tuple.getValueByField("meta");
                byte[] historykey = ArrayUtils.addAll(metricMeta.getUUIDKey(), metricMeta.getKey());
                historykey = ArrayUtils.addAll(historykey, globalFunctions.getDayKey(metricMeta.getErrorState().getTime()));
                
                PutRequest puthistory = new PutRequest(errorshistorytable, historykey, "h".getBytes(), globalFunctions.getNoDayKey(metricMeta.getErrorState().getTime()), metricMeta.getErrorState().getSerialized());
                globalFunctions.getSecindaryclient(clientconf).put(puthistory);
        }catch (IOException ex) {
            LOGGER.error(globalFunctions.stackTrace(ex));                
        }
    }
}

}
