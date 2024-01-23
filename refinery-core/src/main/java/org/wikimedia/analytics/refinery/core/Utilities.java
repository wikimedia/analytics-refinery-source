/**
 * Copyright (C) 2015  Wikimedia Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wikimedia.analytics.refinery.core;

import java.util.Properties;
import java.util.regex.Pattern;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

/**
 * Static functions to work with Wikimedia data, broadly construed;
 * this is where we put the functions generic enough to be reused
 * over and over again in other classes.
 */
public class Utilities {

    /**
     * Check if the target is contained within string.  This is
     * just a convenience method that also makes sure that arguments are not null.
     *
     * @param   string    String to search in
     * @param   target    String to search for
     * @return boolean
     */
    public static boolean stringContains(String string, String target) {
        return (target != null && string != null && string.contains(target));
    }

    /**
     * Convenience method for Using Matcher.find() to check if
     * the given regex Pattern matches the target String.
     * Also called in the LegacyPageview class.
     *
     * @param Pattern pattern
     * @param String  target
     *
     * @return boolean
     */
    public static boolean patternIsFound(Pattern pattern, String target) {

        return (target == null || target.isEmpty()) ? false : pattern.matcher(target).find();

    }

    public static void sendEmail(
        String smtpHost,
        String smtpPort,
        String fromEmail,
        String[] toEmails,
        String subject,
        String body
    ) {
        Properties props = new Properties();
        props.put("mail.smtp.host", smtpHost);
        props.put("mail.smtp.auth", "false");
        props.put("mail.smtp.port", smtpPort);

        Session session = Session.getDefaultInstance(props);

        try {
            MimeMessage message = new MimeMessage(session);
            message.setFrom(new InternetAddress(fromEmail));
            for (String email : toEmails) {
                message.addRecipient(Message.RecipientType.TO, new InternetAddress(email));
            }
            message.setSubject(subject);
            message.setText(body);

            Transport.send(message);
        } catch (MessagingException ex) {
            throw new RuntimeException(ex);
        }
    }

}
