package com.sushil.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch._types.aggregations.DateHistogramBucket;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.itextpdf.text.*;
import com.itextpdf.text.pdf.PdfPCell;
import com.itextpdf.text.pdf.PdfPTable;
import com.itextpdf.text.pdf.PdfWriter;
import jakarta.activation.DataHandler;
import jakarta.activation.DataSource;
import jakarta.activation.FileDataSource;
import jakarta.json.stream.JsonParser;
import jakarta.mail.*;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.*;

public class Main {

	private static final String FROM_EMAIL = ConfigLoader.get("email.from");
	private static final List<String> RECIPIENTS = ConfigLoader.getList("email.recipients");
	private static final String EMAIL_HOST = ConfigLoader.get("email.host");
	private static final String EMAIL_USERNAME = ConfigLoader.get("email.username");
	private static final String EMAIL_PASSWORD = ConfigLoader.get("email.password");
	private static final DateTimeFormatter ISO_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
	private static final String REPORT_TIMESTAMP_FORMAT = ConfigLoader.get("pdf.date.format");
	private static Logger logger = LoggerFactory.getLogger(Main.class);
	private static String REPORT_FROM = "";
	private static String REPORT_TO = "";
	private static final String ENTITIES = ConfigLoader.get("entities");
	private static List<StringTermsBucket> storedBuckets;

	public static void main(String[] args) throws IOException {
		String[] entities = ENTITIES.split(",");
		List<String> pdfFilePaths = new ArrayList<>();
		StringBuilder emailEntitiesList = new StringBuilder();

		String intervalTemplateJson = new String(Files.readAllBytes(Paths.get(ConfigLoader.get("json.template.file.path.interval"))));
		String overallTemplateJson = new String(Files.readAllBytes(Paths.get(ConfigLoader.get("json.template.file.path.overall"))));
		String cpuTemplateJson = new String(Files.readAllBytes(Paths.get(ConfigLoader.get("json.template.file.path.cpu"))));
		String memoryTemplateJson = new String(Files.readAllBytes(Paths.get(ConfigLoader.get("json.template.file.path.memory"))));

		for (String entity : entities) {
			entity = entity.trim().toLowerCase();
			System.out.println(entity);

			String intervalJson = intervalTemplateJson.replace("{{entity}}", entity.toUpperCase());
			String overallJson = overallTemplateJson.replace("{{entity}}", entity.toUpperCase());
			String cpuJson = cpuTemplateJson.replace("{{entity}}", entity.toUpperCase());
			String memoryJson = memoryTemplateJson.replace("{{entity}}", entity.toUpperCase());

			File intervalTempFile = saveToTempFile("interval_" + entity + ".json", intervalJson);
			File overallTempFile = saveToTempFile("overall_" + entity + ".json", overallJson);
			File cpuTempFile = saveToTempFile("cpu_" + entity + ".json", cpuJson);
			File memoryTempFile = saveToTempFile("memory_" + entity + ".json", memoryJson);

			String pdfPath = ConfigLoader.get("pdf.file.path." + entity);

			Map<String, String> jsonFilePathMap = new HashMap<>();
			jsonFilePathMap.put("interval", intervalTempFile.getAbsolutePath());
			jsonFilePathMap.put("overall", overallTempFile.getAbsolutePath());
			jsonFilePathMap.put("cpu", cpuTempFile.getAbsolutePath());
			jsonFilePathMap.put("memory", memoryTempFile.getAbsolutePath());

			Map<String, String> reportData = generateAndSetReportPdf(entity, pdfPath, jsonFilePathMap);

			pdfFilePaths.add(reportData.get("pdfPath"));
			emailEntitiesList.append(entity).append(",");

			intervalTempFile.delete();
			overallTempFile.delete();
		}

		if (emailEntitiesList.length() > 0) {
			emailEntitiesList.deleteCharAt(emailEntitiesList.length() - 1);
		}

		String emailSubject = "Uptime Reports";
		String emailMessage = "Dear Team,\n\nPlease find the attached Uptime Reports for your reference.\n\n"
				+ "Report Period: \n" + REPORT_FROM + " to " + REPORT_TO + "\n\n" + "Entities: "
				+ emailEntitiesList.toString().toUpperCase().replace("_", " ") + "\n\n"
				+ "Let me know if you have any questions.\n\nBest regards";

		System.out.println("EMAIL MESSAGE:-> \n" + emailMessage);
//		 sendEmailWithAttachments(RECIPIENTS, emailSubject,emailMessage,pdfFilePaths);
		
	}

	private static File saveToTempFile(String fileName, String content) throws IOException {
		File tempFile = File.createTempFile(fileName.replace(".json", ""), ".json"); // Ensure correct file format
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {
			writer.write(content);
		}
		return tempFile;
	}

	// ------DATA EXTRACTION AND REPORT GENERATION METHOD---------

	public static Map<String, String> generateAndSetReportPdf(String entityName, String pdfPath,
			Map<String, String> jsonFilePathMap) {
		Map<String, String> reportDates = new HashMap<>();

		try (InputStream intervalJsonStream = new FileInputStream(jsonFilePathMap.get("interval"))) {
			reportDates = addReportDatesToPdfPath(intervalJsonStream);

			String formattedReportFrom = reportDates.get("from").replace(" ", "_").replace(":", "-");
			String formattedReportTo = reportDates.get("to").replace(" ", "_").replace(":", "-");

			String pdfPathWithDates = pdfPath.replace(".pdf",
					"_" + formattedReportFrom + "_to_" + formattedReportTo + ".pdf");

			generatePdfReport(pdfPathWithDates, entityName, jsonFilePathMap);
			reportDates.put("pdfPath", pdfPathWithDates);

		} catch (IOException e) {
			e.printStackTrace();
		}
		return reportDates;
	}

	private static String generatePdfReport(String pdfFilePath, String reportName,
			Map<String, String> jsonFilePathMap) {
		try (FileOutputStream fos = new FileOutputStream(pdfFilePath)) {

			ElasticsearchClient client = ElasticsearchClientFactory.createClient();

			Document document = new Document();
			PdfWriter.getInstance(document, fos);
			document.open();

			addTitleToDocument(document, reportName);

			extractDataFromElasticSearch(client, document, jsonFilePathMap);

			document.close();
			System.out.println("PDF generated successfully: " + pdfFilePath);

			client.close();
			return pdfFilePath;

		} catch (IOException | DocumentException e) {
			logger.error(e.getLocalizedMessage());
			e.printStackTrace();
			return null;
		}
	}

	private static void extractDataFromElasticSearch(ElasticsearchClient client, Document document,
			Map<String, String> jsonFilePathMap) throws IOException, DocumentException {

		//------------------------------
		InputStream queryJsonFile = new FileInputStream(jsonFilePathMap.get("interval"));
		addTimeIntervalToPdfFromJson(queryJsonFile, document);

		//---------------------------------
		InputStream queryStream = new FileInputStream(jsonFilePathMap.get("overall"));
		extractAverageUptime(client, document, queryStream);
		queryJsonFile.close();

		InputStream queryJsonFileForRecords = new FileInputStream(jsonFilePathMap.get("interval"));
		extractAllRecordsInterval(client, document, queryJsonFileForRecords);
		queryJsonFileForRecords.close();

		//----------------------------------
		InputStream thresholdStream = new FileInputStream(jsonFilePathMap.get("cpu"));
		CpuMemoryThreshold.extractCpuThreshold(thresholdStream);
		thresholdStream.close();
		
		InputStream cpuStream = new FileInputStream(jsonFilePathMap.get("cpu"));
		CpuMemoryThreshold.extractCpuUsageDetails(client, document, cpuStream);
		cpuStream.close();
		
		//----------------------------------
		InputStream memoryThresholdStream = new FileInputStream(jsonFilePathMap.get("memory"));
		CpuMemoryThreshold.extractMemoryThreshold(memoryThresholdStream);
		memoryThresholdStream.close();
		
		InputStream memoryStream = new FileInputStream(jsonFilePathMap.get("memory"));
		CpuMemoryThreshold.extractMemoryUsageDetails(client, document, memoryStream);
		memoryStream.close();
		
		addAllRecordsBucketsToDocument(document);
		CpuMemoryThreshold.addCpuUsageDetailsLater(document);
		CpuMemoryThreshold.addMemUsageDetailsLater(document);

	}
	
	private static void extractAverageUptime(ElasticsearchClient client, Document document, InputStream queryStream)
			throws IOException, DocumentException {
		JacksonJsonpMapper jsonpMapper = new JacksonJsonpMapper();
		JsonParser jsonParser = jsonpMapper.jsonProvider().createParser(queryStream);

		SearchRequest searchRequest = SearchRequest.of(b -> b.index("uptime_index").withJson(jsonParser, jsonpMapper));
		SearchResponse<Map> searchResponse = client.search(searchRequest, Map.class);
		Map<String, Aggregate> aggregate = searchResponse.aggregations();
		Aggregate groupByUrlAggregation = aggregate.get("group_by_url");
		List<StringTermsBucket> buckets = groupByUrlAggregation.sterms().buckets().array();

		// Add Section Header
		addStyledSectionHeader(document, "Section A: Uptime Report :");
		addStyledSectionHeader(document, "1: Overall Uptime Average :");

		// Create Table with Better Formatting
		PdfPTable table = new PdfPTable(new float[] { 3, 2 });
		table.setWidthPercentage(100);
		table.setSpacingBefore(5f);
		table.setSpacingAfter(5f);

		table.addCell(createHeaderCell("URL"));
		table.addCell(createHeaderCell("Average Uptime"));

		for (StringTermsBucket bucket : buckets) {
			String url = bucket.key().stringValue();
			Double avgUptime = bucket.aggregations().get("avg_uptime").avg().value();

			table.addCell(new Paragraph(url));

			PdfPCell uptimeCell = new PdfPCell(new Paragraph(String.format("%.2f%%", avgUptime)));
			uptimeCell.setHorizontalAlignment(Element.ALIGN_CENTER);
			uptimeCell.setVerticalAlignment(Element.ALIGN_MIDDLE);

			if (avgUptime < 100) {
				uptimeCell.setBackgroundColor(BaseColor.CYAN);
			}

			table.addCell(uptimeCell);
		}

		document.add(table);
	}

	private static void extractAllRecordsInterval(ElasticsearchClient client, Document document,
			InputStream queryJsonFile) throws IOException, DocumentException {
		JacksonJsonpMapper jsonpMapper = new JacksonJsonpMapper();
		JsonParser jsonParser = jsonpMapper.jsonProvider().createParser(queryJsonFile);

		SearchRequest searchRequest = SearchRequest.of(b -> b.index("uptime_index").withJson(jsonParser, jsonpMapper));
		SearchResponse<Map> searchResponse = client.search(searchRequest, Map.class);

		Map<String, Aggregate> aggregate = searchResponse.aggregations();
		Aggregate groupByUrlAggregation = aggregate.get("group_by_url");
		List<StringTermsBucket> buckets = groupByUrlAggregation.sterms().buckets().array();
		
		// Store for later use
	    storedBuckets = buckets;  

		addStyledSectionHeader(document, "2:Time at which url uptime is less than 100%");
		for (StringTermsBucket bucket : buckets) {
			String url = bucket.key().stringValue();
			Aggregate avgUptimeAggregations = bucket.aggregations().get("hourly_avg");
			List<DateHistogramBucket> avgUptimeBuckets = avgUptimeAggregations.dateHistogram().buckets().array();

			List<DateHistogramBucket> lessThan100Buckets = new ArrayList<>();
			for (DateHistogramBucket datebucket : avgUptimeBuckets) {
				double uptimeValue = datebucket.aggregations().get("avg_uptime").avg().value();
				if (uptimeValue < 100.0) {
					lessThan100Buckets.add(datebucket);
				}
			}

			PdfPTable table = createTableWithUrlHeader(url);
			if (lessThan100Buckets.isEmpty()) {
				addNoDowntimeRow(table);
			} else {
				populateTableWithData(table, lessThan100Buckets);
			}
			document.add(table);
		}

//		addStyledSectionHeader(document, "3: All records between the specified range");
//		for (StringTermsBucket bucket : buckets) {
//			String url = bucket.key().stringValue();
//			Aggregate avgUptimeAggregations = bucket.aggregations().get("hourly_avg");
//			List<DateHistogramBucket> allBuckets = avgUptimeAggregations.dateHistogram().buckets().array();
//
//			PdfPTable table = createTableWithUrlHeader(url);
//			populateTableWithData(table, allBuckets);
//			document.add(table);
//		}
	}

	
	private static void addAllRecordsBucketsToDocument(Document document) throws DocumentException {
	    if (storedBuckets == null || storedBuckets.isEmpty()) {
	        return; // No data stored
	    }

	    addStyledSectionHeader(document, "3: All records between the specified range");
	    for (StringTermsBucket bucket : storedBuckets) {
	        String url = bucket.key().stringValue();
	        Aggregate avgUptimeAggregations = bucket.aggregations().get("hourly_avg");
	        List<DateHistogramBucket> allBuckets = avgUptimeAggregations.dateHistogram().buckets().array();

	        PdfPTable table = createTableWithUrlHeader(url);
	        populateTableWithData(table, allBuckets);
	        document.add(table);
	    }
	}
	
	// ------------DATE FORMATS METHOD--------------------

	public static Map<String, String> addReportDatesToPdfPath(InputStream queryJsonStream) {
		Map<String, String> reportDates = new HashMap<>();
		try {

			ObjectMapper mapper = new ObjectMapper();
			JsonNode rootNode = mapper.readTree(queryJsonStream);

			JsonNode rangeNode = rootNode.path("query").path("bool").path("filter").get(0).path("range").path("@timestamp");
			String reportFromTimestamp = rangeNode.path("gte").asText();
			String reportToTimestamp = rangeNode.path("lt").asText();

			reportDates.put("from", parseDateForPdfPath(reportFromTimestamp));
			reportDates.put("to", parseDateForPdfPath(reportToTimestamp));

		} catch (IOException e) {
			e.printStackTrace();
		}
		return reportDates;
	}

	public static void addTimeIntervalToPdfFromJson(InputStream queryJsonStream, Document document) {
		try {

			ObjectMapper mapper = new ObjectMapper();
			JsonNode rootNode = mapper.readTree(queryJsonStream);
			JsonNode rangeNode = rootNode.path("query").path("bool").path("filter").get(0).path("range").path("@timestamp");
			String reportFromTimestamp = rangeNode.path("gte").asText();
			String reportToTimestamp = rangeNode.path("lt").asText();
			String timeZoneTimestamp = rangeNode.path("time_zone").asText();

			try {
				if (!reportFromTimestamp.equals("now-1d/d")) {
					REPORT_FROM = parseDate(reportFromTimestamp);
				}
				if (!reportToTimestamp.equals("now-1d/d")) {
					REPORT_TO = parseDate(reportFromTimestamp);
				}
				addStyledSectionHeader(document,
						"From: " + parseDate(reportFromTimestamp) + " |  To: " + parseDate(reportToTimestamp));
			} catch (DocumentException e) {
				e.printStackTrace();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static String parseDate(String dateInput) {
		if (dateInput.contains("now")) {
			LocalDate today = LocalDate.now();
			if (dateInput.contains("-")) {
				String[] parts = dateInput.split("-");
				int daysToSubtract = Integer.parseInt(parts[1].replace("d/d", ""));
				REPORT_FROM = formatDate(today.minusDays(daysToSubtract).atStartOfDay().minusHours(5).minusMinutes(30)
						.format(ISO_FORMAT));
				return formatDate(today.minusDays(daysToSubtract).atStartOfDay().minusHours(5).minusMinutes(30)
						.format(ISO_FORMAT));
			} else if (dateInput.equals("now/d")) {
				REPORT_TO = formatDate(
						today.minusDays(0).atStartOfDay().minusHours(5).minusMinutes(30).format(ISO_FORMAT));
				return formatDate(today.minusDays(0).atStartOfDay().minusHours(5).minusMinutes(30).format(ISO_FORMAT));
			}
		} else {
			return formatDate(dateInput);
		}
		return "";
	}

	public static String formatDate(String dateString) {
		Instant instant = Instant.parse(dateString);
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MMM-yyyy hh:mm:ss a")
				.withZone(ZoneId.of("Asia/Kolkata"));
		return formatter.format(instant);
	}

	private static String parseDateForPdfPath(String dateInput) {
		if (dateInput.contains("now")) {
			LocalDate today = LocalDate.now();
			if (dateInput.contains("-")) {
				String[] parts = dateInput.split("-");
				int daysToSubtract = Integer.parseInt(parts[1].replace("d/d", ""));
				REPORT_FROM = formatDateForPdfPath(today.minusDays(daysToSubtract).atStartOfDay().minusHours(5)
						.minusMinutes(30).format(ISO_FORMAT));
				return formatDateForPdfPath(today.minusDays(daysToSubtract).atStartOfDay().minusHours(5)
						.minusMinutes(30).format(ISO_FORMAT));
			} else if (dateInput.equals("now/d")) {
				REPORT_TO = formatDate(
						today.minusDays(0).atStartOfDay().minusHours(5).minusMinutes(30).format(ISO_FORMAT));
				return formatDateForPdfPath(
						today.minusDays(0).atStartOfDay().minusHours(5).minusMinutes(30).format(ISO_FORMAT));
			}
		} else {
			return formatDateForPdfPath(dateInput);
		}
		return "";
	}

	public static String formatDateForPdfPath(String dateString) {
		Instant instant = Instant.parse(dateString);

		DateTimeFormatter defaultFormatter = DateTimeFormatter.ofPattern("EEE_dd-MMM-yyyy hh:mm a").withZone(ZoneId.of("Asia/Kolkata"));

		String pdfDateFormat = ConfigLoader.get("pdf.date.format");
		DateTimeFormatter formatter;
		if (pdfDateFormat != null && !pdfDateFormat.trim().isEmpty()) {
			try {
				formatter = DateTimeFormatter.ofPattern(pdfDateFormat).withZone(ZoneId.of("Asia/Kolkata"));

			} catch (IllegalArgumentException e) {
				System.err.println("Invalid pdf.date.format in configuration. Using default format.");
				formatter = defaultFormatter;
			}
		} else {
			System.err.println("pdf.date.format not provided. Using default format.");
			formatter = defaultFormatter;
		}

		return formatter.format(instant);
	}

	// ----------TABLE FORMAT AND POPULATING METHODS-----------

	public static void addTitleToDocument(Document document, String reportName) throws DocumentException {

		SimpleDateFormat sdf = new SimpleDateFormat("EEEE dd-MMM-yyyy hh:mm:ss a");
		String currentDateTime = sdf.format(new Date());

		Font sectionFont = FontFactory.getFont(FontFactory.HELVETICA_BOLD, 14, BaseColor.BLACK);

		Paragraph title = new Paragraph("Uptime Percentage Report (" + reportName.toUpperCase().replace("_", " ")
				+ ")\nGenerated At: " + currentDateTime + " IST", sectionFont);
		title.setAlignment(Element.ALIGN_CENTER);
		title.setSpacingAfter(20f);

		document.add(title);
	}

	private static PdfPTable createTableWithUrlHeader(String url) throws DocumentException {
		PdfPTable table = new PdfPTable(2);
		table.setWidthPercentage(100);
		table.setSpacingBefore(5f);
		table.setSpacingAfter(5f);
		table.setWidths(new float[] { 3f, 2f });

		PdfPCell urlHeaderCell = new PdfPCell(
				new Paragraph("URL: " + url, FontFactory.getFont(FontFactory.HELVETICA_BOLD)));
		urlHeaderCell.setColspan(2);
		urlHeaderCell.setBackgroundColor(BaseColor.YELLOW);
		urlHeaderCell.setHorizontalAlignment(Element.ALIGN_LEFT);
		urlHeaderCell.setPadding(5f);
		table.addCell(urlHeaderCell);

		table.addCell(createHeaderCell("Timestamp"));
		table.addCell(createHeaderCell("Uptime Percentage"));

		return table;
	}

	private static void addStyledSectionHeader(Document document, String title) throws DocumentException {
		Font sectionFont = FontFactory.getFont(FontFactory.HELVETICA_BOLD, 12, BaseColor.BLACK);
		Paragraph sectionHeader = new Paragraph(title, sectionFont);
		sectionHeader.setSpacingBefore(5f);
		sectionHeader.setSpacingAfter(5f);
		sectionHeader.setAlignment(Element.ALIGN_LEFT);
		document.add(sectionHeader);
	}

	private static void addNoDowntimeRow(PdfPTable table) {
		PdfPCell noDowntimeCell = new PdfPCell(new Paragraph("No Downtime"));
		noDowntimeCell.setColspan(2);
		noDowntimeCell.setHorizontalAlignment(Element.ALIGN_CENTER);
		noDowntimeCell.setPadding(5f);
		table.addCell(noDowntimeCell);
	}

	private static void populateTableWithData(PdfPTable table, List<DateHistogramBucket> buckets) {
		for (DateHistogramBucket bucket : buckets) {
			double uptimeValue = bucket.aggregations().get("avg_uptime").avg().value();
			String timestamp = formatDate(bucket.keyAsString());

			String uptimePercentage = String.format("%.2f%%", uptimeValue);

			table.addCell(new Paragraph(timestamp));
			PdfPCell uptimeCell = new PdfPCell(new Paragraph(uptimePercentage));
			uptimeCell.setHorizontalAlignment(Element.ALIGN_CENTER);
			uptimeCell.setVerticalAlignment(Element.ALIGN_MIDDLE);

			if (uptimeValue < 100) {
				uptimeCell.setBackgroundColor(BaseColor.CYAN);
			}

			table.addCell(uptimeCell);
		}
	}

	private static PdfPCell createHeaderCell(String text) {
		PdfPCell cell = new PdfPCell(new Paragraph(text, FontFactory.getFont(FontFactory.HELVETICA_BOLD)));
		cell.setHorizontalAlignment(Element.ALIGN_CENTER);
		cell.setVerticalAlignment(Element.ALIGN_MIDDLE);
		cell.setBackgroundColor(BaseColor.LIGHT_GRAY);
		cell.setPadding(5f);
		return cell;
	}

	// ------------EMAIL METHOD-----------

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
