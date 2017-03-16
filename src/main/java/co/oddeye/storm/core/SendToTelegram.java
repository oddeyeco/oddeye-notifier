/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm.core;

import co.oddeye.core.OddeeyMetricMeta;
import co.oddeye.core.OddeeySenderMetricMetaList;
import co.oddeye.core.OddeyeHttpURLConnection;
import co.oddeye.core.globalFunctions;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class SendToTelegram extends SendTo {

    public static final Logger LOGGER = LoggerFactory.getLogger(SendToTelegram.class);
    private final OddeeySenderMetricMetaList targetdata;
    private final Map.Entry<String, StormUser> targetuser;

    public SendToTelegram(OddeeySenderMetricMetaList value, Map.Entry<String, StormUser> user) {
        targetdata = value;
        targetuser = user;
    }

    @Override
    public void run() {
        LOGGER.warn("Sent for user " + targetuser.getValue().getEmail() + " to Telegram " + targetdata.size() + " Messages");
        Iterator<Map.Entry<Integer, OddeeyMetricMeta>> iter = targetdata.entrySet().iterator();
        String Text = "";
        int Counter = 0;
        while (iter.hasNext()) {
            Counter++;
            Map.Entry<Integer, OddeeyMetricMeta> entry = iter.next();
            if (Counter < 11) {
                if (entry.getValue().getErrorState().getLevel() == -1) {
                    Text = Text + "\nMertic:" + entry.getValue().getName() + "\nTags:\n   " + entry.getValue().getDisplayTags("\n   ") + " Already not in Error ";
                } else {
                    Text = Text + "\nLevel For Metric:" + entry.getValue().getName() + "\nTags:\n   " + entry.getValue().getDisplayTags("\n   ") + " " + entry.getValue().getErrorState().getStateName() + " to " + entry.getValue().getErrorState().getLevelName();
                }
            }                            

            iter.remove();
        }
        LOGGER.warn(Text);
        if ((!targetdata.getTargetValue().isEmpty()) && (!Text.isEmpty())) {
            if (Counter>10)
            {
                Text = Text+"\n And "+(Counter-10)+" More";
            }
            try {
                Text = "OddEye Report:" + Text;
                Text = URLEncoder.encode(Text, "UTF-8");
                String uri = "https://api.telegram.org/bot317219245:AAFqFjcddeXfrpIZ-V4ENeve87oxg0ZGGYs/sendMessage?chat_id=" + targetdata.getTargetValue() + "&text=" + Text;
                OddeyeHttpURLConnection.sendGet(uri);
            } catch (UnsupportedEncodingException ex) {
                LOGGER.error(globalFunctions.stackTrace(ex));
            } catch (Exception ex) {
                LOGGER.error(globalFunctions.stackTrace(ex));
            }
        }

    }

}
