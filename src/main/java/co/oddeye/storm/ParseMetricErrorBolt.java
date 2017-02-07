/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm;

import co.oddeye.core.ErrorState;
import co.oddeye.core.OddeeyMetricMeta;
import co.oddeye.core.OddeeyMetricMetaList;
import co.oddeye.core.globalFunctions;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.hbase.async.GetRequest;
import org.hbase.async.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class ParseMetricErrorBolt extends BaseRichBolt {

    protected OutputCollector collector;
    public static final Logger LOGGER = LoggerFactory.getLogger(ParseMetricErrorBolt.class);
    private JsonParser parser = null;
    private final Map conf;
    private Config openTsdbConfig;
    private org.hbase.async.Config clientconf;
    private byte[] metatable;
    private OddeeyMetricMetaList mtrscList;

    public ParseMetricErrorBolt(java.util.Map config) {
        this.conf = config;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("meta","reaction","startvalue"));
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
            mtrscList = new OddeeyMetricMetaList();
        } catch (IOException ex) {
            LOGGER.error("ERROR: " + globalFunctions.stackTrace(ex));
        }
    }

    @Override
    public void execute(Tuple input) {
        String msg = input.getString(0);        
        try {
            OddeeyMetricMeta metricMeta;
            final JsonElement ErrorData;
            ErrorData = this.parser.parse(msg);
            int hash = ErrorData.getAsJsonObject().get("hash").getAsInt();
            if (mtrscList.get(hash)==null) {
                byte[] key = Hex.decodeHex(ErrorData.getAsJsonObject().get("key").getAsString().toCharArray());
                GetRequest request = new GetRequest(metatable, key, "d".getBytes());
                ArrayList<KeyValue> row = globalFunctions.getSecindaryclient(clientconf).get(request).joinUninterruptibly();
//                metric = new OddeeyMetricMeta(row, globalFunctions.getSecindarytsdb(openTsdbConfig, clientconf), false);
                metricMeta = new OddeeyMetricMeta(row, globalFunctions.getSecindarytsdb(openTsdbConfig, clientconf), false);                                
                LOGGER.info(metricMeta.getName() + " " + ErrorData);
            }
            else
            {
                metricMeta = mtrscList.get(hash);
            }
            ErrorState errorState = new ErrorState(ErrorData.getAsJsonObject());
            metricMeta.setErrorState(errorState);
            Integer reaction = ErrorData.getAsJsonObject().get("reaction").getAsInt();
            Double startvalue = ErrorData.getAsJsonObject().get("startvalue").getAsDouble();
            this.collector.emit(new Values(metricMeta,reaction,startvalue));
            mtrscList.set(metricMeta);
        } catch (JsonSyntaxException e) {
            LOGGER.error("JsonSyntaxException: " + globalFunctions.stackTrace(e)+" "+msg);
        } catch (DecoderException ex) {
            LOGGER.error("DecoderException: " + globalFunctions.stackTrace(ex)+" "+msg);
        } catch (Exception ex) {
            LOGGER.error("Exception: " + globalFunctions.stackTrace(ex)+" "+msg);
        }
        this.collector.ack(input);

    }

}
