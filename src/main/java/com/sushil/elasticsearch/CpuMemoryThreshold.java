package com.sushil.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import com.itextpdf.text.*;
import com.itextpdf.text.pdf.PdfPCell;
import com.itextpdf.text.pdf.PdfPTable;
import jakarta.json.stream.JsonParser;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;
import java.util.*;

public class CpuMemoryThreshold {

	private static Aggregate fetchCpuUsageData(ElasticsearchClient client, InputStream queryStream) throws IOException {
		JacksonJsonpMapper jsonpMapper = new JacksonJsonpMapper();
		JsonParser jsonParser = jsonpMapper.jsonProvider().createParser(queryStream);
		SearchRequest searchRequest = SearchRequest
				.of(b -> b.index("metricbeat_index").withJson(jsonParser, jsonpMapper));
		SearchResponse<Map> searchResponse = client.search(searchRequest, Map.class);
		return searchResponse.aggregations().get("group_by_hostname");
	}

	public static void extractCpuUsageDetails(ElasticsearchClient client, Document document, InputStream queryStream)
			throws IOException, DocumentException {

		Aggregate hostAgg = fetchCpuUsageData(client, queryStream);
		if (hostAgg == null || !hostAgg.isSterms())
			return;

		addStyledSectionHeader(document, "B: CPU Usage Details");
		addStyledSectionHeader(document, "Time Range at which threshold(20%) exceeds continuously more than 5 min");

		boolean extendedColumns = false;
		List<StringTermsBucket> buckets = hostAgg.sterms().buckets().array();
		for (StringTermsBucket bucket : buckets) {
			if (bucket.key().stringValue().startsWith("DR"))
				continue;

			Aggregate highCpuUsageAgg = bucket.aggregations().get("high_cpu_usage");
			if (highCpuUsageAgg.isTopHits()) {
				List<Map<String, Object>> records = highCpuUsageAgg.topHits().hits().hits().stream()
						.map(hit -> hit.source().to(Map.class))
						.sorted(Comparator.comparing(rec -> Instant.parse((String) rec.get("@timestamp"))))
						.collect(Collectors.toList());

				List<List<Map<String, Object>>> continuousBuckets = groupRecordsByTimeGap(records);
				for (List<Map<String, Object>> bucketRecords : continuousBuckets) {
					if (bucketRecords.size() > 1) {
						Instant first = Instant.parse((String) bucketRecords.get(0).get("@timestamp"));
						Instant last = Instant
								.parse((String) bucketRecords.get(bucketRecords.size() - 1).get("@timestamp"));
						if (Duration.between(first, last).toMinutes() >= 5) {
							extendedColumns = true;
							break;
						}
					}
				}
			}
			if (extendedColumns)
				break;
		}

		PdfPTable thresholdTable = extendedColumns ? new PdfPTable(new float[] { 3, 3, 3, 2, 2 })
				: new PdfPTable(new float[] { 3, 3, 3 });

		thresholdTable.setWidthPercentage(100);
		thresholdTable.setSpacingBefore(5f);
		thresholdTable.setSpacingAfter(10f);

		thresholdTable.addCell(createHeaderCell("Hostname"));
		thresholdTable.addCell(createHeaderCell("Start At"));
		thresholdTable.addCell(createHeaderCell("End At"));
		if (extendedColumns) {
			thresholdTable.addCell(createHeaderCell("Avg CPU"));
			thresholdTable.addCell(createHeaderCell("Max CPU"));
		}

		DateTimeFormatter dtFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy hh:mm:ss a");
		ZoneId istZone = ZoneId.of("Asia/Kolkata");

		if (buckets.isEmpty()) {
			PdfPCell noThresholdCell = new PdfPCell(new Paragraph("No servers reached the threshold",
					FontFactory.getFont(FontFactory.HELVETICA_BOLD, 12)));
			noThresholdCell.setColspan(extendedColumns ? 5 : 3);
			noThresholdCell.setHorizontalAlignment(Element.ALIGN_CENTER);
			noThresholdCell.setPadding(5f);
			thresholdTable.addCell(noThresholdCell);
		} else {
			for (StringTermsBucket bucket : buckets) {
				String hostname = bucket.key().stringValue();
				if (hostname.startsWith("DR"))
					continue;

				Aggregate highCpuUsageAgg = bucket.aggregations().get("high_cpu_usage");
				boolean anyThresholdFound = false;

				if (highCpuUsageAgg.isTopHits()) {
					List<Map<String, Object>> records = highCpuUsageAgg.topHits().hits().hits().stream()
							.map(hit -> hit.source().to(Map.class))
							.sorted(Comparator.comparing(rec -> Instant.parse((String) rec.get("@timestamp"))))
							.collect(Collectors.toList());

					List<List<Map<String, Object>>> continuousBuckets = groupRecordsByTimeGap(records);
					for (List<Map<String, Object>> bucketRecords : continuousBuckets) {
						if (bucketRecords.size() > 1) {
							Instant first = Instant.parse((String) bucketRecords.get(0).get("@timestamp"));
							Instant last = Instant
									.parse((String) bucketRecords.get(bucketRecords.size() - 1).get("@timestamp"));
							if (Duration.between(first, last).toMinutes() >= 5) {
								anyThresholdFound = true;

								thresholdTable.addCell(createDataCell(hostname));
								thresholdTable.addCell(createDataCell(first.atZone(istZone).format(dtFormatter)));
								thresholdTable.addCell(createDataCell(last.atZone(istZone).format(dtFormatter)));

								if (extendedColumns) {
									Map<String, Double> cpuUsageStats = fetchCpuUsageStats(client, hostname, first,
											last);
									double avgCpu = cpuUsageStats.getOrDefault("avg", 0.0);
									double maxCpu = cpuUsageStats.getOrDefault("max", 0.0);
									thresholdTable.addCell(createDataCell(String.format("%.2f%%", avgCpu)));
									thresholdTable.addCell(createDataCell(String.format("%.2f%%", maxCpu)));
								}
							}
						}
					}
				}

				if (!anyThresholdFound) {
					PdfPCell noRecordCell = new PdfPCell(
							new Paragraph("No threshold reached", FontFactory.getFont(FontFactory.HELVETICA_BOLD, 12)));
					noRecordCell.setColspan(extendedColumns ? 2 : 2);
					noRecordCell.setHorizontalAlignment(Element.ALIGN_CENTER);
					noRecordCell.setPadding(5f);

					thresholdTable.addCell(createDataCell(hostname));
					thresholdTable.addCell(noRecordCell);
				}
			}
		}

		document.add(thresholdTable);
		 addStyledSectionHeader(document, "Section 2: Details of CPU usage exceeding threshold above 20%");
		 generateCpuUsageDetailTable(document, buckets, dtFormatter, istZone);
	}

	private static void generateCpuUsageDetailTable(Document document, List<StringTermsBucket> buckets,
			DateTimeFormatter dtFormatter, ZoneId istZone) throws DocumentException {

		for (StringTermsBucket bucket : buckets) {
			String hostname = bucket.key().stringValue();
			if (hostname.startsWith("DR"))
				continue;

			PdfPTable cpuTable = new PdfPTable(new float[] { 3, 2, 2, 2 });
			cpuTable.setWidthPercentage(100);
			cpuTable.setSpacingBefore(10f);
			cpuTable.setSpacingAfter(15f);

			PdfPCell hostCell = new PdfPCell(
					new Paragraph("Hostname: " + hostname, FontFactory.getFont(FontFactory.HELVETICA_BOLD, 12)));
			hostCell.setColspan(4);
			hostCell.setHorizontalAlignment(Element.ALIGN_CENTER);
			hostCell.setBackgroundColor(BaseColor.YELLOW);
			hostCell.setPadding(8f);
			cpuTable.addCell(hostCell);

			cpuTable.addCell(createHeaderCell("Timestamp"));
			cpuTable.addCell(createHeaderCell("User CPU (%)"));
			cpuTable.addCell(createHeaderCell("System CPU (%)"));
			cpuTable.addCell(createHeaderCell("Total CPU (%)"));

			Aggregate highCpuUsageAgg = bucket.aggregations().get("high_cpu_usage");
//			System.out.println(highCpuUsageAgg);
			if (highCpuUsageAgg.isTopHits()) {
				List<Map<String, Object>> records = highCpuUsageAgg.topHits().hits().hits().stream()
						.map(hit -> hit.source().to(Map.class))
						.sorted(Comparator.comparing(rec -> Instant.parse((String) rec.get("@timestamp"))))
						.collect(Collectors.toList());

				for (Map<String, Object> rec : records) {
					String timestamp = (String) rec.get("@timestamp");
					Instant ts = Instant.parse(timestamp);
					String formattedTs = ts.atZone(istZone).format(dtFormatter);

					double userCpuPct = extractCpuUsage(rec, "user");
					double systemCpuPct = extractCpuUsage(rec, "system");
					double totalCpuPct = extractCpuUsage(rec, "total");

					cpuTable.addCell(createDataCell(formattedTs));
					cpuTable.addCell(createDataCell(String.format("%.2f", userCpuPct)));
					cpuTable.addCell(createDataCell(String.format("%.2f", systemCpuPct)));
					cpuTable.addCell(createDataCell(String.format("%.2f", totalCpuPct)));
				}
			}
			document.add(cpuTable);
		}
	}

	private static double extractCpuUsage(Map<String, Object> record, String key) {
		return Optional.ofNullable((Map<String, Object>) record.getOrDefault("system", Collections.emptyMap()))
				.map(system -> (Map<String, Object>) system.getOrDefault("cpu", Collections.emptyMap()))
				.map(cpu -> (Map<String, Object>) cpu.getOrDefault(key, Collections.emptyMap()))
				.map(usage -> (Map<String, Number>) usage.getOrDefault("norm", Collections.emptyMap()))
				.map(norm -> norm.getOrDefault("pct", 0.0).doubleValue() * 100).orElse(0.0);
	}

	public static Map<String, Double> fetchCpuUsageStats(ElasticsearchClient client, String hostname, Instant gte,
			Instant lt) throws IOException {
		Map<String, Double> stats = new HashMap<>();

		// Define date-time formatter for IST
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy hh:mm:ss a")
				.withZone(ZoneId.of("Asia/Kolkata"));

		// Format timestamps
		String formattedGte = formatter.format(gte);
		String formattedLt = formatter.format(lt);

		// Print formatted timestamps
		System.out.println("Start Time (GTE): " + formattedGte);
		System.out.println("End Time (LT): " + formattedLt);

		// Read JSON query from file
		Path jsonPath = Paths.get("src/main/resources/query_json/hardware.json");
		String statsJson = new String(Files.readAllBytes(jsonPath), StandardCharsets.UTF_8);

		// Replace placeholders dynamically
		String queryJson = statsJson.replace("{{gte}}", gte.toString()).replace("{{lt}}", lt.toString());

		// Convert JSON string to InputStream
		InputStream queryStream = new ByteArrayInputStream(queryJson.getBytes(StandardCharsets.UTF_8));
		JacksonJsonpMapper jsonpMapper = new JacksonJsonpMapper();
		JsonParser jsonParser = jsonpMapper.jsonProvider().createParser(queryStream);

		// Send search request
		SearchRequest searchRequest = SearchRequest
				.of(b -> b.index("metricbeat_index").withJson(jsonParser, jsonpMapper));
		SearchResponse<Map> searchResponse = client.search(searchRequest, Map.class);
		System.out.println(searchResponse);

		// Extract aggregation results
		Map<String, Aggregate> aggregate = searchResponse.aggregations();
		Aggregate groupByHostNameAggregation = aggregate.get("group_by_hostname");

		if (groupByHostNameAggregation == null || groupByHostNameAggregation.sterms() == null) {
			System.out.println("No aggregation data found.");
			return stats;
		}

		List<StringTermsBucket> buckets = groupByHostNameAggregation.sterms().buckets().array();

		// Iterate over each bucket (each hostname)
		for (StringTermsBucket bucket : buckets) {
			String hostnameByBucket = bucket.key().stringValue();

			if (hostname.equals(hostnameByBucket)) { // Corrected hostname comparison
				Aggregate cpuTotalUsage = bucket.aggregations().getOrDefault("cpu_total_usage", null);

				if (cpuTotalUsage != null && cpuTotalUsage.stats() != null) {
					double avgCpu = cpuTotalUsage.stats().avg() * 100;
					double maxCpu = cpuTotalUsage.stats().max() * 100;

					stats.put("avg", avgCpu);
					stats.put("max", maxCpu);
				}
				break; // Stop looping once the correct hostname is found
			}
		}

		return stats;
	}

	// --------MEMORY--------------------

	private static Aggregate fetchMemoryUsageData(ElasticsearchClient client, InputStream queryStream)
			throws IOException {
		JacksonJsonpMapper jsonpMapper = new JacksonJsonpMapper();
		JsonParser jsonParser = jsonpMapper.jsonProvider().createParser(queryStream);
		SearchRequest searchRequest = SearchRequest
				.of(b -> b.index("metricbeat_index").withJson(jsonParser, jsonpMapper));
		SearchResponse<Map> searchResponse = client.search(searchRequest, Map.class);
		return searchResponse.aggregations().get("group_by_hostname");
	}

	public static void extractMemoryUsageDetails(ElasticsearchClient client, Document document, InputStream queryStream)
			throws IOException, DocumentException {
		Aggregate hostAgg = fetchMemoryUsageData(client, queryStream);
		if (hostAgg == null || !hostAgg.isSterms())
			return;

		addStyledSectionHeader(document, "C: Memory Usage Details");
		addStyledSectionHeader(document, "Time Range at which threshold(20%) exceeds continuously more than 5 min");

		PdfPTable thresholdTable = new PdfPTable(new float[] { 3, 3, 3 });
		thresholdTable.setWidthPercentage(100);
		thresholdTable.setSpacingBefore(5f);
		thresholdTable.setSpacingAfter(10f);

		thresholdTable.addCell(createHeaderCell("Hostname"));
		thresholdTable.addCell(createHeaderCell("Threshold Start At"));
		thresholdTable.addCell(createHeaderCell("Threshold End At"));

		boolean thresholdFound = false;
		DateTimeFormatter dtFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy hh:mm:ss a");
		ZoneId istZone = ZoneId.of("Asia/Kolkata");

		List<StringTermsBucket> buckets = hostAgg.sterms().buckets().array();
		if (buckets.isEmpty()) {
			PdfPCell noThresholdCell = new PdfPCell(new Paragraph("No servers reached the threshold",
					FontFactory.getFont(FontFactory.HELVETICA_BOLD, 12)));
			noThresholdCell.setColspan(3);
			noThresholdCell.setHorizontalAlignment(Element.ALIGN_CENTER);
			noThresholdCell.setPadding(5f);
			thresholdTable.addCell(noThresholdCell);
		} else {
			for (StringTermsBucket bucket : buckets) {
				String hostname = bucket.key().stringValue();
				if (hostname.startsWith("DR"))
					continue;

				Aggregate highMemoryUsageAgg = bucket.aggregations().get("high_memory_usage");
				boolean anyThresholdFound = false;

				if (highMemoryUsageAgg.isTopHits()) {
					List<Map<String, Object>> records = highMemoryUsageAgg.topHits().hits().hits().stream()
							.map(hit -> hit.source().to(Map.class))
							.sorted(Comparator.comparing(rec -> Instant.parse((String) rec.get("@timestamp"))))
							.collect(Collectors.toList());

					List<List<Map<String, Object>>> continuousBuckets = groupRecordsByTimeGap(records);
					for (List<Map<String, Object>> bucketRecords : continuousBuckets) {
						if (bucketRecords.size() > 1) {
							Instant first = Instant.parse((String) bucketRecords.get(0).get("@timestamp"));
							Instant last = Instant
									.parse((String) bucketRecords.get(bucketRecords.size() - 1).get("@timestamp"));
							if (Duration.between(first, last).toMinutes() >= 5) {
								thresholdFound = true;
								anyThresholdFound = true;

								thresholdTable.addCell(createDataCell(hostname));
								thresholdTable.addCell(createDataCell(first.atZone(istZone).format(dtFormatter)));
								thresholdTable.addCell(createDataCell(last.atZone(istZone).format(dtFormatter)));
							}
						}
					}
				}

				if (!anyThresholdFound) {
					thresholdTable.addCell(createDataCell(hostname));
					PdfPCell noRecordCell = new PdfPCell(
							new Paragraph("No threshold reached", FontFactory.getFont(FontFactory.HELVETICA_BOLD, 12)));
					noRecordCell.setColspan(2);
					noRecordCell.setHorizontalAlignment(Element.ALIGN_CENTER);
					noRecordCell.setPadding(5f);
					thresholdTable.addCell(noRecordCell);
				}
			}
		}

		document.add(thresholdTable);
	    addStyledSectionHeader(document, "Section 2: Details of Memory usage exceeding threshold above 80%");
	    generateMemoryUsageDetailTable(document, buckets, dtFormatter, istZone);
	}

	private static void generateMemoryUsageDetailTable(Document document, List<StringTermsBucket> buckets,
			DateTimeFormatter dtFormatter, ZoneId istZone) throws DocumentException {
		for (StringTermsBucket bucket : buckets) {
			String hostname = bucket.key().stringValue();
			if (hostname.startsWith("DR"))
				continue;

			PdfPTable memoryTable = new PdfPTable(new float[] { 3, 2, 2 });
			memoryTable.setWidthPercentage(100);
			memoryTable.setSpacingBefore(10f);
			memoryTable.setSpacingAfter(15f);

			PdfPCell hostCell = new PdfPCell(
					new Paragraph("Hostname: " + hostname, FontFactory.getFont(FontFactory.HELVETICA_BOLD, 12)));
			hostCell.setColspan(3);
			hostCell.setHorizontalAlignment(Element.ALIGN_CENTER);
			hostCell.setBackgroundColor(BaseColor.YELLOW);
			hostCell.setPadding(8f);
			memoryTable.addCell(hostCell);

			memoryTable.addCell(createHeaderCell("Timestamp"));
			memoryTable.addCell(createHeaderCell("Swap Memory(%)"));
			memoryTable.addCell(createHeaderCell("Actual Memory (%)"));

			Aggregate highMemoryUsageAgg = bucket.aggregations().get("high_memory_usage");

			if (highMemoryUsageAgg.isTopHits()) {
				List<Map<String, Object>> records = highMemoryUsageAgg.topHits().hits().hits().stream()
						.map(hit -> hit.source().to(Map.class))
						.sorted(Comparator.comparing(rec -> Instant.parse((String) rec.get("@timestamp"))))
						.collect(Collectors.toList());

				for (Map<String, Object> rec : records) {
					String timestamp = (String) rec.get("@timestamp");
					Instant ts = Instant.parse(timestamp);
					String formattedTs = ts.atZone(istZone).format(dtFormatter);

					double swapMemoryPct = extractMemoryUsage(rec, "used.pct");
					double actualMemoryPct = extractMemoryUsage(rec, "actual.used.pct");

					memoryTable.addCell(createDataCell(formattedTs));
					memoryTable.addCell(createDataCell(String.format("%.2f", swapMemoryPct)));
					memoryTable.addCell(createDataCell(String.format("%.2f", actualMemoryPct)));
				}
			}
			document.add(memoryTable);
		}
	}

	private static double extractMemoryUsage(Map<String, Object> record, String key) {

		return Optional.ofNullable(record).map(r -> (Map<String, Object>) r.get("system"))
				.map(system -> (Map<String, Object>) system.get("memory")).map(memory -> key.contains("."))
				.map(nested -> {
					if (nested) {
						return Optional
								.ofNullable((Map<String, Object>) ((Map<String, Object>) record.get("system"))
										.get("memory"))
								.map(memory -> (Map<String, Object>) memory.get("actual"))
								.map(actual -> (Map<String, Number>) actual.get("used")).map(used -> used.get("pct"))
								.map(Number::doubleValue).map(value -> value * 100).orElse(98.0);
					} else {

						return Optional
								.ofNullable((Map<String, Object>) ((Map<String, Object>) record.get("system"))
										.get("memory"))
								.map(memory -> (Number) memory.get("used.pct")).map(Number::doubleValue)
								.map(value -> value * 100).orElse(97.0);
					}
				}).orElse(99.0);

	}

	// -----------------

	private static List<List<Map<String, Object>>> groupRecordsByTimeGap(List<Map<String, Object>> records) {
		List<List<Map<String, Object>>> continuousBuckets = new ArrayList<>();
		List<Map<String, Object>> currentBucket = new ArrayList<>();
		Instant lastTimestamp = null;

		for (Map<String, Object> record : records) {
			Instant currentTimestamp = Instant.parse((String) record.get("@timestamp"));
			if (currentBucket.isEmpty() || Duration.between(lastTimestamp, currentTimestamp).getSeconds() <= 10) {
				currentBucket.add(record);
			} else {
				continuousBuckets.add(new ArrayList<>(currentBucket));
				currentBucket.clear();
				currentBucket.add(record);
			}
			lastTimestamp = currentTimestamp;
		}
		if (!currentBucket.isEmpty())
			continuousBuckets.add(currentBucket);
		return continuousBuckets;
	}

	private static PdfPCell createDataCell(String text) {
		PdfPCell cell = new PdfPCell(new Paragraph(text, FontFactory.getFont(FontFactory.HELVETICA, 12)));
		cell.setHorizontalAlignment(Element.ALIGN_CENTER);
		cell.setVerticalAlignment(Element.ALIGN_MIDDLE);
		cell.setPadding(5f);
		return cell;
	}

	private static PdfPCell createHeaderCell(String text) {
		PdfPCell cell = new PdfPCell(new Paragraph(text, FontFactory.getFont(FontFactory.HELVETICA_BOLD)));
		cell.setHorizontalAlignment(Element.ALIGN_CENTER);
		cell.setVerticalAlignment(Element.ALIGN_MIDDLE);
		cell.setBackgroundColor(BaseColor.LIGHT_GRAY);
		cell.setPadding(5f);
		return cell;
	}

	private static void addStyledSectionHeader(Document document, String title) throws DocumentException {
		Font sectionFont = FontFactory.getFont(FontFactory.HELVETICA_BOLD, 12, BaseColor.BLACK);
		Paragraph sectionHeader = new Paragraph(title, sectionFont);
		sectionHeader.setSpacingBefore(3f);
		sectionHeader.setSpacingAfter(3f);
		sectionHeader.setAlignment(Element.ALIGN_LEFT);
		document.add(sectionHeader);
	}

	// ------------------------
	public static void getHardwareReports() throws IOException {
		ElasticsearchClient client = ElasticsearchClientFactory.createClient();
		JacksonJsonpMapper jsonpMapper = new JacksonJsonpMapper();
		InputStream queryStream = new FileInputStream("src\\main\\resources\\query_json\\hardware.json");
		JsonParser jsonParser = jsonpMapper.jsonProvider().createParser(queryStream);

		SearchRequest searchRequest = SearchRequest
				.of(b -> b.index("metricbeat_index").withJson(jsonParser, jsonpMapper));
		SearchResponse<Map> searchResponse = client.search(searchRequest, Map.class);
//		logger.warn(searchResponse + " ");
		Map<String, Aggregate> aggregate = searchResponse.aggregations();
		Aggregate groupByHostNameAggregation = aggregate.get("group_by_hostname");

		List<StringTermsBucket> buckets = groupByHostNameAggregation.sterms().buckets().array();
		System.out.println("---------");

		// Iterate over each bucket (each hostname)
		for (StringTermsBucket bucket : buckets) {
			String hostname = bucket.key().stringValue();
			Aggregate cpuUserUsage = bucket.aggregations().get("cpu_user_usage");
			Aggregate cpuSystemUsage = bucket.aggregations().get("cpu_system_usage");
			Aggregate cpuTotalUsage = bucket.aggregations().get("cpu_total_usage");
			Aggregate memoryUsage = bucket.aggregations().get("memory_usage");
			Aggregate diskUsage = bucket.aggregations().get("disk_usage");

			// Extract min, max, avg values for CPU User Usage
			double cpuUserMin = Math.max(cpuUserUsage.stats().min() * 100, 0.0);
			double cpuUserMax = cpuUserUsage.stats().max() * 100;
			double cpuUserAvg = cpuUserUsage.stats().avg() * 100;

			// Extract min, max, avg values for CPU System Usage
			double cpuSystemMin = Math.max(cpuUserUsage.stats().min() * 100, 0.0);
			double cpuSystemMax = cpuSystemUsage.stats().max() * 100;
			double cpuSystemAvg = cpuSystemUsage.stats().avg() * 100;

			// Extract min, max, avg values for CPU System Usage
			double cpuTotalMin = Math.max(cpuUserUsage.stats().min() * 100, 0.0);
			double cpuTotalMax = cpuTotalUsage.stats().max() * 100;
			double cpuTotalAvg = cpuTotalUsage.stats().avg() * 100;

			// Extract min, max, avg values for Memory Usage
			double memoryMin = Math.max(cpuUserUsage.stats().min() * 100, 0.0);
			double memoryMax = memoryUsage.stats().max() * 100;
			double memoryAvg = memoryUsage.stats().avg() * 100;

			// Extract min, max, avg values for Disk Usage
			double diskMin = Math.max(cpuUserUsage.stats().min() * 100, 0.0);
			double diskMax = diskUsage.stats().max() * 100;
			double diskAvg = diskUsage.stats().avg() * 100;

			// Log or use these values
			System.out.println("Hostname: " + hostname);
			System.out.println("CPU User Min: " + String.format("%.2f", cpuUserMin) + ", Max: "
					+ String.format("%.2f", cpuUserMax) + ", Avg: " + String.format("%.2f", cpuUserAvg));
			System.out.println("CPU System Min: " + String.format("%.2f", cpuSystemMin) + ", Max: "
					+ String.format("%.2f", cpuSystemMax) + ", Avg: " + String.format("%.2f", cpuSystemAvg));
			System.out.println("CPU Total Min: " + String.format("%.2f", cpuTotalMin) + ", Max: "
					+ String.format("%.2f", cpuTotalMax) + ", Avg: " + String.format("%.2f", cpuTotalAvg));
			System.out.println("Memory Min: " + String.format("%.2f", memoryMin) + ", Max: "
					+ String.format("%.2f", memoryMax) + ", Avg: " + String.format("%.2f", memoryAvg));
//		    System.out.println("Disk Min: " + diskMin + ", Max: " + diskMax + ", Avg: " + diskAvg);
			System.out.println("---------");
		}

		queryStream.close();
		client.close();

	}

}
