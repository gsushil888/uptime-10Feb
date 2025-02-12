package com.sushil.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

	private static final String ENVIRONMENT_PREFIX = "DR";
	private static final String METRICBEAT_INDEX = "metricbeat_index";

	private static final String SECTION_HEADER_CPU_USAGE = "Section B: CPU Usage Details";
	private static String SECTION_HEADER_CPU_EXCEEDS = "Timestamp where the cpu exceeds 20% for over 5 minutes";
	private static String SECTION_HEADER_CPU_ALL_DETAILS = "Details of CPU usage exceeding above 20%";

	private static final String SECTION_HEADER_MEMORY_USAGE = "Section C: MEMORY Usage Details";
	private static String SECTION_HEADER_MEMORY_EXCEEDS = "Timestamp where the memory exceeds 80% for over 5 minutes";
	private static String SECTION_HEADER_MEMORY_ALL_DETAILS = "Details of Memory usage exceeding above 80%";

	private static final String NO_THRESHOLD_MESSAGE = "No servers reached the threshold";
	private static final String NO_RECORD_MESSAGE = "No threshold reached";
	private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("dd-MM-yyyy hh:mm:ss a");
	private static final ZoneId IST_ZONE = ZoneId.of("Asia/Kolkata");
	private static final List<String> CPU_TABLE_HEADERS = Arrays.asList("Timestamp", "User CPU (%)", "System CPU (%)",
			"Total CPU (%)");
	
	private static List<StringTermsBucket> cpustoredBuckets;
	private static List<StringTermsBucket> memstoredBuckets;

	private static Aggregate fetchCpuUsageData(ElasticsearchClient client, InputStream queryStream) throws IOException {

		JacksonJsonpMapper jsonpMapper = new JacksonJsonpMapper();
		JsonParser jsonParser = jsonpMapper.jsonProvider().createParser(queryStream);
		SearchRequest searchRequest = SearchRequest
				.of(b -> b.index(METRICBEAT_INDEX).withJson(jsonParser, jsonpMapper));
		SearchResponse<Map> searchResponse = client.search(searchRequest, Map.class);
		return searchResponse.aggregations().get("group_by_hostname");
	}

	public static void extractCpuUsageDetails(ElasticsearchClient client, Document document, InputStream queryStream)
			throws IOException, DocumentException {

		Aggregate hostAgg = fetchCpuUsageData(client, queryStream);
		if (hostAgg == null || !hostAgg.isSterms())
			return;

		addStyledSectionHeader(document, SECTION_HEADER_CPU_USAGE);
		addStyledSectionHeader(document, SECTION_HEADER_CPU_EXCEEDS);

		boolean extendedColumns = false;
		List<StringTermsBucket> buckets = hostAgg.sterms().buckets().array();

		for (StringTermsBucket bucket : buckets) {
			if (bucket.key().stringValue().startsWith(ENVIRONMENT_PREFIX))
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

		if (buckets.isEmpty()) {
			PdfPCell noThresholdCell = new PdfPCell(
					new Paragraph(NO_THRESHOLD_MESSAGE, FontFactory.getFont(FontFactory.HELVETICA_BOLD, 12)));
			noThresholdCell.setColspan(extendedColumns ? 5 : 3);
			noThresholdCell.setHorizontalAlignment(Element.ALIGN_CENTER);
			noThresholdCell.setPadding(5f);
			thresholdTable.addCell(noThresholdCell);
		} else {
			for (StringTermsBucket bucket : buckets) {
				String hostname = bucket.key().stringValue();
				if (hostname.startsWith(ENVIRONMENT_PREFIX))
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
								thresholdTable
										.addCell(createDataCell(first.atZone(IST_ZONE).format(DATE_TIME_FORMATTER)));
								thresholdTable
										.addCell(createDataCell(last.atZone(IST_ZONE).format(DATE_TIME_FORMATTER)));
								if (extendedColumns) {
									Map<String, Double> cpuUsageStats = fetchCpuUsageStats(client, hostname, first,
											last);
									thresholdTable.addCell(createDataCell(
											String.format("%.2f%%", cpuUsageStats.getOrDefault("avg", 0.0))));
									thresholdTable.addCell(createDataCell(
											String.format("%.2f%%", cpuUsageStats.getOrDefault("max", 0.0))));
								}
							}
						}
					}
				}

				if (!anyThresholdFound) {
					PdfPCell noRecordCell = new PdfPCell(
							new Paragraph(NO_RECORD_MESSAGE, FontFactory.getFont(FontFactory.HELVETICA_BOLD, 12)));
					noRecordCell.setColspan(extendedColumns ? 4 : 4);
					noRecordCell.setHorizontalAlignment(Element.ALIGN_CENTER);
					noRecordCell.setPadding(5f);

					thresholdTable.addCell(createDataCell(hostname));
					thresholdTable.addCell(noRecordCell);
				}
			}
		}

		document.add(thresholdTable);
//		generateCpuUsageDetailTable(document, buckets, DATE_TIME_FORMATTER, IST_ZONE);

		// Store the details table content and generate it later when required
		storeCpuUsageDetailsForLater(buckets);

	}

	private static void generateCpuUsageDetailTable(Document document, List<StringTermsBucket> buckets,
			DateTimeFormatter dtFormatter, ZoneId istZone) throws DocumentException {

		System.out.println("CPU DETAILS:  " + buckets);
		addStyledSectionHeader(document, SECTION_HEADER_CPU_ALL_DETAILS);

		if (buckets.isEmpty()) {
			PdfPTable cpuTable = new PdfPTable(new float[] { 3, 2, 2, 2 });
			cpuTable.setWidthPercentage(100);
			cpuTable.setSpacingBefore(10f);
			cpuTable.setSpacingAfter(15f);
			// Add header cells
			for (String header : CPU_TABLE_HEADERS) {
				cpuTable.addCell(createHeaderCell(header));
			}
			// If no buckets, show a message
			PdfPCell noDataCell = new PdfPCell(
					new Paragraph("No threshold reached", FontFactory.getFont(FontFactory.HELVETICA_BOLD, 12)));
			noDataCell.setColspan(CPU_TABLE_HEADERS.size());
			noDataCell.setHorizontalAlignment(Element.ALIGN_CENTER);
			noDataCell.setPadding(5f);
			cpuTable.addCell(noDataCell);
			document.add(cpuTable);
			return;
		}

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
			hostCell.setColspan(CPU_TABLE_HEADERS.size());
			hostCell.setHorizontalAlignment(Element.ALIGN_CENTER);
			hostCell.setBackgroundColor(BaseColor.YELLOW);
			hostCell.setPadding(8f);
			cpuTable.addCell(hostCell);

			// Add header cells
			for (String header : CPU_TABLE_HEADERS) {
				cpuTable.addCell(createHeaderCell(header));
			}

			Aggregate highCpuUsageAgg = bucket.aggregations().get("high_cpu_usage");
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
				break;
			}
		}

		return stats;
	}

	public static double extractCpuThreshold(InputStream jsonStream) throws IOException {
		ObjectMapper objectMapper = new ObjectMapper();
		JsonNode rootNode = objectMapper.readTree(jsonStream);

		double userCpuThreshold = 0.0;
		double systemCpuThreshold = 0.0;

		// Navigate to the "should" array
		JsonNode shouldArray = rootNode.path("query").path("bool").path("should");
		if (shouldArray.isArray()) {
			for (JsonNode condition : shouldArray) {
				JsonNode rangeNode = condition.path("range");

				if (rangeNode.has("system.cpu.user.norm.pct")) {
					userCpuThreshold = rangeNode.path("system.cpu.user.norm.pct").path("gte").asDouble(0.0);
				}

				if (rangeNode.has("system.cpu.system.norm.pct")) {
					systemCpuThreshold = rangeNode.path("system.cpu.system.norm.pct").path("gte").asDouble(0.0);
				}
			}
		}
		SECTION_HEADER_CPU_EXCEEDS = SECTION_HEADER_CPU_EXCEEDS.replace("20%",
				String.format("%.0f%%", Math.max(userCpuThreshold, systemCpuThreshold) * 100));
		SECTION_HEADER_CPU_ALL_DETAILS = SECTION_HEADER_CPU_ALL_DETAILS.replace("20%",
				String.format("%.0f%%", Math.max(userCpuThreshold, systemCpuThreshold) * 100));
		// Return the maximum threshold found
		return Math.max(userCpuThreshold, systemCpuThreshold);
	}

	// New method to store CPU details for later
	private static void storeCpuUsageDetailsForLater(List<StringTermsBucket> buckets) {
		cpustoredBuckets = buckets; // store for later use (define storedBuckets as a static variable or in a
									// context object)
	}

	// Call this method later when you want to add the details table
	public static void addCpuUsageDetailsLater(Document document) throws DocumentException {
		if (cpustoredBuckets != null && !cpustoredBuckets.isEmpty()) {
			generateCpuUsageDetailTable(document, cpustoredBuckets, DATE_TIME_FORMATTER, IST_ZONE);
		}
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

		addStyledSectionHeader(document, SECTION_HEADER_MEMORY_USAGE);
		addStyledSectionHeader(document, SECTION_HEADER_MEMORY_EXCEEDS);

		boolean extendedColumns = false;
		List<StringTermsBucket> buckets = hostAgg.sterms().buckets().array();

		for (StringTermsBucket bucket : buckets) {
			if (bucket.key().stringValue().startsWith("DR"))
				continue;
			Aggregate highMemoryUsageAgg = bucket.aggregations().get("high_memory_usage");
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
			thresholdTable.addCell(createHeaderCell("Avg Memory"));
			thresholdTable.addCell(createHeaderCell("Max Memory"));
		}

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
								anyThresholdFound = true;
								thresholdTable.addCell(createDataCell(hostname));
								thresholdTable
										.addCell(createDataCell(first.atZone(IST_ZONE).format(DATE_TIME_FORMATTER)));
								thresholdTable
										.addCell(createDataCell(last.atZone(IST_ZONE).format(DATE_TIME_FORMATTER)));
								if (extendedColumns) {
									Map<String, Double> memoryUsageStats = fetchMemoryUsageStats(client, hostname,
											first, last);
									thresholdTable.addCell(createDataCell(
											String.format("%.2f%%", memoryUsageStats.getOrDefault("avg", 0.0))));
									thresholdTable.addCell(createDataCell(
											String.format("%.2f%%", memoryUsageStats.getOrDefault("max", 0.0))));
								}
							}
						}
					}
				}

				if (!anyThresholdFound) {
					PdfPCell noRecordCell = new PdfPCell(
							new Paragraph("No threshold reached", FontFactory.getFont(FontFactory.HELVETICA_BOLD, 12)));
					noRecordCell.setColspan(extendedColumns ? 4 : 3);
					noRecordCell.setHorizontalAlignment(Element.ALIGN_CENTER);
					noRecordCell.setPadding(5f);

					thresholdTable.addCell(createDataCell(hostname));
					thresholdTable.addCell(noRecordCell);
				}
			}
		}

		document.add(thresholdTable);
//		generateMemoryUsageDetailTable(document, buckets, DATE_TIME_FORMATTER, IST_ZONE);
		
		storeMemUsageDetailsForLater(buckets);
	}

	private static void generateMemoryUsageDetailTable(Document document, List<StringTermsBucket> buckets,
			DateTimeFormatter dtFormatter, ZoneId istZone) throws DocumentException {
		System.out.println("MEMORY DETAILS: " + buckets);
		addStyledSectionHeader(document, SECTION_HEADER_MEMORY_ALL_DETAILS);

		if (buckets.isEmpty()) {

			PdfPTable memoryTable = new PdfPTable(new float[] { 3, 2, 2 });
			memoryTable.setWidthPercentage(100);
			memoryTable.setSpacingBefore(10f);
			memoryTable.setSpacingAfter(15f);

			memoryTable.addCell(createHeaderCell("Timestamp"));
			memoryTable.addCell(createHeaderCell("Swap Memory(%)"));
			memoryTable.addCell(createHeaderCell("Actual Memory (%)"));

			// If no buckets, show a message
			PdfPCell noDataCell = new PdfPCell(
					new Paragraph("No threshold reached", FontFactory.getFont(FontFactory.HELVETICA_BOLD, 12)));
			noDataCell.setColspan(4);
			noDataCell.setHorizontalAlignment(Element.ALIGN_CENTER);
			noDataCell.setPadding(5f);
			memoryTable.addCell(noDataCell);
			document.add(memoryTable);
			return;
		}
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

					double swapMemoryPct = extractUsedMemory(rec);
					double actualMemoryPct = extractActualUsedMemory(rec);

					memoryTable.addCell(createDataCell(formattedTs));
					memoryTable.addCell(createDataCell(String.format("%.2f", swapMemoryPct)));
					memoryTable.addCell(createDataCell(String.format("%.2f", actualMemoryPct)));
				}
			}
			document.add(memoryTable);
		}
	}

	private static double extractActualUsedMemory(Map<String, Object> record) {
	    if (record == null) return 99.0;

	    Map<String, Object> system = (Map<String, Object>) record.get("system");
	    if (system == null) return 99.0;

	    Map<String, Object> memory = (Map<String, Object>) system.get("memory");
	    if (memory == null) return 99.0;

	    return Optional.ofNullable((Map<String, Object>) memory.get("actual"))
	            .map(actual -> (Map<String, Number>) actual.get("used"))
	            .map(used -> used.get("pct"))
	            .map(Number::doubleValue)
	            .map(value -> value * 100)
	            .orElse(98.0);
	}

	private static double extractUsedMemory(Map<String, Object> record) {
	    if (record == null) return 99.0;

	    Map<String, Object> system = (Map<String, Object>) record.get("system");
	    if (system == null) return 99.0;

	    Map<String, Object> memory = (Map<String, Object>) system.get("memory");
	    if (memory == null) return 99.0;

	    return Optional.ofNullable(memory.get("used"))
	            .map(used -> ((Map<String, Number>) used).get("pct"))
	            .map(Number::doubleValue)
	            .map(value -> value * 100)
	            .orElse(97.0);
	}
	
	


	public static Map<String, Double> fetchMemoryUsageStats(ElasticsearchClient client, String hostname, Instant gte,
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
				Aggregate memTotalUsage = bucket.aggregations().getOrDefault("memory_usage", null);

				if (memTotalUsage != null && memTotalUsage.stats() != null) {
					double avgMem = memTotalUsage.stats().avg() * 100;
					double maxMem = memTotalUsage.stats().max() * 100;

					stats.put("avg", avgMem);
					stats.put("max", maxMem);
				}
				break;
			}
		}

		return stats;
	}

	public static double extractMemoryThreshold(InputStream jsonStream) throws IOException {
		ObjectMapper objectMapper = new ObjectMapper();
		JsonNode rootNode = objectMapper.readTree(jsonStream);

		double memoryThreshold = 0.0;

		// Navigate to the "should" array
		JsonNode shouldArray = rootNode.path("query").path("bool").path("should");
		if (shouldArray.isArray()) {
			for (JsonNode condition : shouldArray) {
				JsonNode rangeNode = condition.path("range");

				if (rangeNode.has("system.memory.actual.used.pct")) {
					memoryThreshold = rangeNode.path("system.memory.actual.used.pct").path("gte").asDouble(0.0);
				}
			}
		}

		SECTION_HEADER_MEMORY_EXCEEDS = SECTION_HEADER_MEMORY_EXCEEDS.replace("80%",
				String.format("%.0f%%", memoryThreshold * 100));
		SECTION_HEADER_MEMORY_ALL_DETAILS = SECTION_HEADER_MEMORY_ALL_DETAILS.replace("80%",
				String.format("%.0f%%", memoryThreshold * 100));
		return memoryThreshold;
	}
	

	// New method to store CPU details for later
	private static void storeMemUsageDetailsForLater(List<StringTermsBucket> buckets) {
		memstoredBuckets = buckets; // store for later use (define storedBuckets as a static variable or in a
									// context object)
	}

	// Call this method later when you want to add the details table
	public static void addMemUsageDetailsLater(Document document) throws DocumentException {
		if (memstoredBuckets != null && !memstoredBuckets.isEmpty()) {
			generateMemoryUsageDetailTable(document, memstoredBuckets, DATE_TIME_FORMATTER, IST_ZONE);
		}
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

}
