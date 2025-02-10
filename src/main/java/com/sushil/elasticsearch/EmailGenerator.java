package com.sushil.elasticsearch;

import java.io.File;
import java.util.List;
import java.util.Properties;

import jakarta.activation.DataHandler;
import jakarta.activation.DataSource;
import jakarta.activation.FileDataSource;
import jakarta.mail.Authenticator;
import jakarta.mail.BodyPart;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.Multipart;
import jakarta.mail.PasswordAuthentication;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;

public class EmailGenerator {
	
	private static final String FROM_EMAIL = ConfigLoader.get("email.from");
	private static final List<String> RECIPIENTS = ConfigLoader.getList("email.recipients");
	private static final String EMAIL_HOST = ConfigLoader.get("email.host");
	private static final String EMAIL_USERNAME = ConfigLoader.get("email.username");
	private static final String EMAIL_PASSWORD = ConfigLoader.get("email.password");
	
	
	public static void sendEmail(StringBuilder emailEntitiesList,String REPORT_FROM,String REPORT_TO,List<String> pdfFilePaths) {

		String emailSubject = "Uptime Reports";
		String emailMessage = "Dear Team,\n\nPlease find the attached Uptime Reports for your reference.\n\n"
				+ "Report Period: \n" + REPORT_FROM + " to " + REPORT_TO + "\n\n" + "Entities: "
				+ emailEntitiesList.toString().toUpperCase().replace("_", " ") + "\n\n"
				+ "Let me know if you have any questions.\n\nBest regards";

		System.out.println("EMAIL MESSAGE:-> \n" + emailMessage);
		 sendEmailWithAttachments(RECIPIENTS, emailSubject,emailMessage,pdfFilePaths);
	}
	
	public static void sendEmailWithAttachments(List<String> toRecipients, String subject, String body,
			List<String> filePaths) {

		Properties properties = System.getProperties();
		properties.put("mail.smtp.host", EMAIL_HOST);
		properties.put("mail.smtp.port", "587");
		properties.put("mail.smtp.auth", "true");
		properties.put("mail.smtp.starttls.enable", "true");
		properties.put("mail.smtp.ssl.enable.enable", "true");
		properties.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");

		Session session = Session.getInstance(properties, new Authenticator() {
			@Override
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(EMAIL_USERNAME, EMAIL_PASSWORD);
			}
		});

		try {
			MimeMessage message = new MimeMessage(session);
			message.setFrom(new InternetAddress(FROM_EMAIL));

			for (String recipient : toRecipients) {
				message.addRecipient(Message.RecipientType.TO, new InternetAddress(recipient));
			}
			message.setSubject(subject);

			BodyPart messageBodyPart = new MimeBodyPart();
			messageBodyPart.setText(body);

			Multipart multipart = new MimeMultipart();
			multipart.addBodyPart(messageBodyPart);

			for (String filePath : filePaths) {
				if (filePath != null) {
					MimeBodyPart attachmentPart = new MimeBodyPart();
					DataSource source = new FileDataSource(filePath);
					attachmentPart.setDataHandler(new DataHandler(source));
					attachmentPart.setFileName(new File(filePath).getName());
					multipart.addBodyPart(attachmentPart);
				}
			}

			message.setContent(multipart);

			Transport.send(message);
			System.out.println("Email sent successfully to : " + RECIPIENTS);
		} catch (MessagingException mex) {
			System.err.println("Mail sending failure");
			mex.printStackTrace();
		}
	}

}
