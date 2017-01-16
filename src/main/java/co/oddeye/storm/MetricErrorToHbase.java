/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm;

import co.oddeye.core.OddeeyMetricMeta;
import co.oddeye.core.globalFunctions;
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
    public void execute(Tuple input) {
//        String msg = input.getString(0);        
        try {
            OddeeyMetricMeta metric = (OddeeyMetricMeta) input.getValueByField("metric");
            String message = (String) input.getValueByField("message");
//Bytes.toString(val)
            byte[] historykey = ArrayUtils.addAll(globalFunctions.getDayKey(metric.getErrorState().getTime()), metric.getUUIDKey());
            byte[] lastkey = metric.getUUIDKey();
            //+" timekey:"+Hex.encodeHexString(globalFunctions.getNoDayKey(metric.getErrorState().getTime()))
            if (metric.getErrorState().getMessage() == null) {
                qualifiers = new byte[5][];
                values = new byte[5][];
            } else {
                qualifiers = new byte[6][];
                values = new byte[6][];
            }

            if (metric.getErrorState().getLevel() > -1) {
                qualifiers[0] = "level".getBytes();
                qualifiers[1] = "time".getBytes();
                qualifiers[2] = "starttimes".getBytes();
                qualifiers[3] = "endtimes".getBytes();
                qualifiers[4] = "action".getBytes();
                if (metric.getErrorState().getMessage() != null) {
                    qualifiers[5] = "message".getBytes();
                    values[5] = metric.getErrorState().getMessage().getBytes();
                }
                values[0] = ByteBuffer.allocate(1).put((byte) metric.getErrorState().getLevel()).array();
                values[4] = ByteBuffer.allocate(1).put((byte) metric.getErrorState().getState()).array();
                values[1] = ByteBuffer.allocate(8).putLong(metric.getErrorState().getTime()).array();
                ByteBuffer buffer = ByteBuffer.allocate(metric.getErrorState().getStarttimes().size() + metric.getErrorState().getStarttimes().size() * 8);
                for (Map.Entry<Integer, Long> time : metric.getErrorState().getStarttimes().entrySet()) {
                    int level = time.getKey();
                    buffer.put((byte) level).putLong(time.getValue());
                }
                values[2] = buffer.array();

                buffer.clear();
                buffer = ByteBuffer.allocate(metric.getErrorState().getEndtimes().size() + metric.getErrorState().getEndtimes().size() * 8);
                for (Map.Entry<Integer, Long> time : metric.getErrorState().getEndtimes().entrySet()) {
                    int level = time.getKey();
                    buffer.put((byte) level).putLong(time.getValue());
                }
                values[3] = buffer.array();

                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("metric:" + metric.getName() + " Host:" + metric.getTags().get("host").getValue() + " Err:" + metric.getTags().get("UUID").getValue() + " state:" + metric.getErrorState().getState() + " time:" + metric.getErrorState().getTime() + " daykey:" + Hex.encodeHexString(lastkey) + " message:" + message);
                }
                PutRequest putlast = new PutRequest(errorslasttable, lastkey, "l".getBytes(), qualifiers, values);
                globalFunctions.getSecindaryclient(clientconf).put(putlast);
            } else {
                final DeleteRequest delreq = new DeleteRequest(errorslasttable, lastkey);
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Delete key:" + Hex.encodeHexString(lastkey));
                }
                globalFunctions.getSecindaryclient(clientconf).delete(delreq);
            }

            PutRequest puthistory = new PutRequest(errorshistorytable, historykey, "h".getBytes(), globalFunctions.getNoDayKey(metric.getErrorState().getTime()), ByteBuffer.allocate(2).put((byte) metric.getErrorState().getLevel()).put((byte) metric.getErrorState().getState()).array());
            globalFunctions.getSecindaryclient(clientconf).put(puthistory);

        } catch (Exception ex) {
            LOGGER.error("ERROR: " + globalFunctions.stackTrace(ex));
        }
        this.collector.ack(input);

    }

}
