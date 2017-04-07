/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm;

import co.oddeye.core.OddeeyMetricMeta;
import co.oddeye.core.OddeeySenderMetricMetaList;
import co.oddeye.core.globalFunctions;
import co.oddeye.storm.core.SendToEmail;
import co.oddeye.storm.core.SendToTelegram;
import co.oddeye.storm.core.StormUser;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class SendNotifierBolt extends BaseRichBolt {

    private final Map conf;
    public static final Logger LOGGER = LoggerFactory.getLogger(SendNotifierBolt.class);
    private OutputCollector collector;
    private Config openTsdbConfig;
    private org.hbase.async.Config clientconf;
    private byte[] userstable;
    private JsonParser parser;
    private Map<String, StormUser> UserList;
    private Map<Integer, OddeeyMetricMeta> ErrorsList;
    private ExecutorService executor;
    private Map<String, Object> mailconf;

    public SendNotifierBolt(java.util.Map config) {
        this.conf = config;
    }

    SendNotifierBolt(Map<String, Object> TSDBconfig, Map<String, Object> Mailconfig) {
        this.conf = TSDBconfig;
        this.mailconf = Mailconfig;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("metric"));
    }

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        try {
            parser = new JsonParser();
            UserList = new HashMap<>();
            ErrorsList = new HashMap<>();
            executor = Executors.newFixedThreadPool(3);
            LOGGER.warn("DoPrepare SendNotifierBolt");
            collector = oc;
            String quorum = String.valueOf(conf.get("zkHosts"));
            LOGGER.warn("quorum: " + quorum);
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
            this.userstable = String.valueOf(conf.get("userstable")).getBytes();
            final Scanner user_scanner = globalFunctions.getSecindaryclient(clientconf).newScanner(userstable);
            ArrayList<ArrayList<KeyValue>> rows;
            while ((rows = user_scanner.nextRows(1000).joinUninterruptibly()) != null) {
                for (final ArrayList<KeyValue> row : rows) {
                    final StormUser User = new StormUser(row, parser);
                    UserList.put(User.getId().toString(), User);
                }
            }
            LOGGER.warn("UserList.size " + UserList.size());
        } catch (IOException ex) {
            LOGGER.error("ERROR: " + globalFunctions.stackTrace(ex));
        } catch (Exception ex) {
            LOGGER.error("ERROR: " + globalFunctions.stackTrace(ex));
        }
    }

    @Override
    public void execute(Tuple tuple) {
        if (tuple.getSourceComponent().equals("kafkaSemaphoreSpot")) {
            JsonObject jsonResult = this.parser.parse(tuple.getString(0)).getAsJsonObject();
            if (jsonResult.get("action").getAsString().equals("changefilter")) {
                String filter = jsonResult.get("filter").getAsString();
                UserList.get(jsonResult.get("UUID").getAsString()).getFiltertemplateList().put(jsonResult.get("filtername").getAsString(), filter);
                Map<String, String> map = new HashMap<>();
                map = (Map<String, String>) globalFunctions.getGson().fromJson(filter, map.getClass());
                UserList.get(jsonResult.get("UUID").getAsString()).getFiltertemplateMap().put(jsonResult.get("filtername").getAsString(), map);
            }
        }
        if (tuple.getSourceComponent().equals("TimerSpout")) {
//            LOGGER.warn("Ancav mi rope");
            UserList.entrySet().forEach((Map.Entry<String, StormUser> user) -> {
                user.getValue().getTargetList().entrySet().stream().map((Map.Entry<String, OddeeySenderMetricMetaList> target) -> {
                    Runnable Sender = null;
                    if (target.getValue().size() > 0) {
//                        LOGGER.warn("Ancav mi rope "+target.getKey());
                        if (target.getKey().equals("telegram")) {
                            Sender = new SendToTelegram(target.getValue(), user);
                        }
                        if (target.getKey().equals("email")) {
                            Sender = new SendToEmail(target.getValue(), user,mailconf);
                        }
                    }
                    return Sender;
                }).filter((Sender) -> (Sender != null)).forEachOrdered((Sender) -> {
                    executor.submit(Sender);
                });
            });
        }

        if (tuple.getSourceComponent().equals("ParseMetricBolt")) {
            OddeeyMetricMeta metricMeta = (OddeeyMetricMeta) tuple.getValueByField("meta");
            final StormUser User = UserList.get(metricMeta.getTags().get("UUID").getValue());
            if (ErrorsList.containsKey(metricMeta.hashCode())) {
                User.PrepareNotifier(metricMeta, ErrorsList.get(metricMeta.hashCode()), false);
                if (metricMeta.getErrorState().getLevel() == -1) {
                    ErrorsList.remove(metricMeta.hashCode());
                }
            } else {
                if (metricMeta.getErrorState().getLevel() > -1) {
                    User.PrepareNotifier(metricMeta, null, true);
                }
            }
            ErrorsList.put(metricMeta.hashCode(), metricMeta);
        }

        collector.ack(tuple);
    }

}
