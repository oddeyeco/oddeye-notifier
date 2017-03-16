/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm.core;

import co.oddeye.core.OddeeyMetricMeta;
import co.oddeye.core.OddeeySenderMetricMetaList;
import co.oddeye.core.globalFunctions;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import javax.mail.Authenticator;
import javax.mail.Session;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Transport;

import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class SendToEmail extends SendTo {

    public static final Logger LOGGER = LoggerFactory.getLogger(SendToEmail.class);
    private final OddeeySenderMetricMetaList targetdata;
    private final Map.Entry<String, StormUser> targetuser;
    private final Session session;
    private final String from;

    public SendToEmail(OddeeySenderMetricMetaList value, Map.Entry<String, StormUser> user) {
        targetdata = value;
        targetuser = user;

        // Sender's email ID needs to be mentioned
        from = "noreply@oddeye.co";

        // Assuming you are sending email from localhost
        String host = "mail.netangels.net";

        // Get system properties
        Properties properties = new Properties();

        // Setup mail server
        properties.put("mail.smtp.auth", "true");
//        props.put("mail.smtp.starttls.enable", "true");

        properties.put("mail.smtp.port", "25");

        properties.put("mail.smtp.host", host);
        properties.put("mail.user", "noreply@oddeye.co");
        properties.put("mail.password", "Rembo3Rembo4");

        // Get the default Session object.
        String username = "noreply@oddeye.co";
        String password = "Rembo3Rembo4";        
        session = Session.getInstance(properties, new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(username, password);
            }
        });        
    }

    @Override
    public void run() {
        LOGGER.warn("Sent for user " + targetuser.getValue().getEmail() + " to Email " + targetdata.size() + " Messages");
        Iterator<Map.Entry<Integer, OddeeyMetricMeta>> iter = targetdata.entrySet().iterator();
        String Text = "";

        while (iter.hasNext()) {

            Map.Entry<Integer, OddeeyMetricMeta> entry = iter.next();

            if (entry.getValue().getErrorState().getLevel() == -1) {
                Text = "<div>" + Text + "<br>Mertic:" + entry.getValue().getName() + "<br>Tags:<br>" + entry.getValue().getDisplayTags("<br>") + " Already not in Error " + "</div>";
            } else {
                Text = "<div>" + Text + "<br>Level For Metric:" + entry.getValue().getName() + "<br>Tags:<br>" + entry.getValue().getDisplayTags("<br>") + " " + entry.getValue().getErrorState().getStateName() + " to " + entry.getValue().getErrorState().getLevelName() + "</div>";
            }

            iter.remove();
        }
        LOGGER.warn(Text);
        if (!Text.isEmpty()) {
            // Recipient's email ID needs to be mentioned.
            String to = targetdata.getTargetValue();
            LOGGER.warn("targetdata.getTargetValue(); " + targetdata.getTargetValue());
            try {
                MimeMessage message = new MimeMessage(session);

                // Set From: header field of the header.
                message.setFrom(new InternetAddress(from,"OddEye Analytic"));

                // Set To: header field of the header.
                message.addRecipient(Message.RecipientType.TO, new InternetAddress(to));

                // Set Subject: header field
                message.setSubject("OddEye Report");

                // Send the actual HTML message, as big as you like
                message.setContent(Text, "text/html");
//                message.setHeader("Message-ID", Math.random()+"@oddeye.co");

// Send message
                Transport.send(message);
                LOGGER.warn("mail sended");
            } catch (MessagingException ex) {
                LOGGER.error(globalFunctions.stackTrace(ex));
            } catch (UnsupportedEncodingException ex) {
                java.util.logging.Logger.getLogger(SendToEmail.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else {
        }

    }

}
