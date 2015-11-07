package com.rtmap.mon.bcia;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class MailSender {
    public static final String MAIL_USERNAME = "datasupport@rtmap.com";
    public static final String MAIL_PASSWORD = "111111@rtmap.com";
    public static final String MAIL_FROMADDR = "datasupport@rtmap.com";

    public static boolean sendHtmlMail(String toAddr, String subject, String content) {
        return sendHtmlMail(MAIL_USERNAME, MAIL_PASSWORD, MAIL_FROMADDR, toAddr, subject, content);
    }

    public static boolean sendTextMail(String toAddr, String subject, String content) {
        return sendTextMail(MAIL_USERNAME, MAIL_PASSWORD, MAIL_FROMADDR, toAddr, subject, content);
    }

    public static boolean sendHtmlMail(String username, String passwd, String fromAddr, String toAddr, String subject, String content) {
        Properties p = new Properties();
        //p.put("mail.smtp.host", "smtp.ym.163.com");
        p.put("mail.smtp.host", "smtp.mxhichina.com");
        p.put("mail.smtp.port", "25");
        p.put("mail.smtp.auth", "true");
        MyAuthenticator authenticator = new MyAuthenticator(username, passwd);

        Session sendMailSession = Session.getDefaultInstance(p, authenticator);
        try {

            Message mailMessage = new MimeMessage(sendMailSession);

            Address from = new InternetAddress(fromAddr);

            mailMessage.setFrom(from);

            String[] recivers = toAddr.split(",");
            Address[] tos = new Address[recivers.length];
            for (int i = 0; i < recivers.length; i++) {
                tos[i] = new InternetAddress(recivers[i]);
            }

            mailMessage.setRecipients(Message.RecipientType.TO, tos);

            mailMessage.setSubject(subject);

            mailMessage.setSentDate(new Date());

            Multipart mainPart = new MimeMultipart();

            BodyPart html = new MimeBodyPart();

            html.setContent(content, "text/html; charset=utf-8");
            mainPart.addBodyPart(html);

            mailMessage.setContent(mainPart);
            //mailMessage.setText(content);

            Transport.send(mailMessage);
            return true;
        } catch (MessagingException ex) {
            ex.printStackTrace();
        }
        return false;
    }

    public static boolean sendTextMail(String username, String passwd, String fromAddr, String toAddr, String subject, String content) {

        Properties p = new Properties();
        //p.put("mail.smtp.host", "smtp.ym.163.com");
        p.put("mail.smtp.host", "smtp.mxhichina.com");
        p.put("mail.smtp.port", "25");
        p.put("mail.smtp.auth", "true");
        MyAuthenticator authenticator = new MyAuthenticator(username, passwd);

        Session sendMailSession = Session.getDefaultInstance(p, authenticator);
        try {

            Message mailMessage = new MimeMessage(sendMailSession);

            Address from = new InternetAddress(fromAddr);

            mailMessage.setFrom(from);

            String[] recivers = toAddr.split(",");
            Address[] tos = new Address[recivers.length];
            for (int i = 0; i < recivers.length; i++) {
                tos[i] = new InternetAddress(recivers[i]);
            }

            mailMessage.setRecipients(Message.RecipientType.TO, tos);

            mailMessage.setSubject(subject);

            mailMessage.setSentDate(new Date());
            mailMessage.setText(content);

            Transport.send(mailMessage);
            return true;
        } catch (MessagingException ex) {
            ex.printStackTrace();
        }
        return false;
    }

    public static class MyAuthenticator extends Authenticator {
        String userName = null;
        String password = null;

        public MyAuthenticator() {
        }

        public MyAuthenticator(String username, String password) {
            this.userName = username;
            this.password = password;
        }

        protected PasswordAuthentication getPasswordAuthentication() {
            return new PasswordAuthentication(userName, password);
        }
    }

    public static String buildMailContent(List<String> tbHeader, List<List<String>> tbBody) {
        return buildMailContent(tbHeader, tbBody, true);
    }

    /**
     * 
     *
     * @param tbHeader
     * @param tbBody
     * @return
     */
    public static String buildMailContent(List<String> tbHeader, List<List<String>> tbBody, boolean isLinkShow) {
        StringBuffer sb = new StringBuffer();
        sb.append("<br/><div style='margin-left:10px;'><table><thead><tr>");
        for (String head : tbHeader) {
            sb.append("<th style='padding:3px 6px;border-bottom:2px solid rgb(221, 221, 221);'>").append(head).append("</th>");
        }
        sb.append("</tr></thead><tbody>");
        for (List<String> bodyList : tbBody) {
            sb.append("<tr>");
            for (String body : bodyList) {
                sb.append("<td style='padding:3px 6px;border-bottom:1px solid rgb(221, 221, 221);'>").append(body).append("</td>");
            }
            sb.append("</tr>");
        }
        sb.append("</tbody></table></div><br/>");
        /*if (isLinkShow) {
            sb.append("<br/><span style='margin-left:10px;'>鐐瑰嚮璁块棶锛�a href='http://192.168.1.203:8080/agent'>鐩戞帶骞冲彴</a>")
                    .append("&nbsp;&nbsp;&nbsp;&nbsp;<a href='http://192.168.1.250:8188/platform/'>鏁版嵁骞冲彴</a></span>");
        }*/
        return sb.toString();
    }

    public static void main(String[] args) {
    	StringBuffer sb = new StringBuffer();
        try{
        	String pyCmd = String.format("%s %s", "python", "monitor_bcia.py");
            Process pr = Runtime.getRuntime().exec(pyCmd);

            String line;
            BufferedReader in = new BufferedReader(new InputStreamReader(pr.getInputStream()));
            while ((line = in.readLine()) != null) {
            	line = String.format("%s\n", line);
            	sb.append(line);
            }
            in.close();
            pr.waitFor();
        } catch (Exception e){
            e.printStackTrace();
        }

        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date();

        String title = String.format("bcia-queue monitoring report %s", sdf.format(date));
        String content = sb.toString();
        sendTextMail("wangjianbin@rtmap.com,liujialin@rtmap.com,zhangxiangwei@rtmap.com,243798275@qq.com,260497070@qq.com,411588524@qq.com", title, content);
    }
}
