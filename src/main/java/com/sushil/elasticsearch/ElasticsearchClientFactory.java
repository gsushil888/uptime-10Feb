package com.sushil.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

public class ElasticsearchClientFactory {

	private static final String ELASTIC_USERNAME = ConfigLoader.get("elastic.username");
	private static final String ELASTIC_PASSWORD = ConfigLoader.get("elastic.password");
	private static final String ELASTIC_HOST = ConfigLoader.get("elastic.host");
	private static final int ELASTIC_PORT = ConfigLoader.getInt("elastic.port");

	public static ElasticsearchClient createClient() {
		BasicCredentialsProvider credsProv = new BasicCredentialsProvider();
		credsProv.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(ELASTIC_USERNAME, ELASTIC_PASSWORD));

		RestClientBuilder builder = RestClient.builder(new HttpHost(ELASTIC_HOST, ELASTIC_PORT, "http"))
				.setHttpClientConfigCallback(
						httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credsProv));

		RestClient restClient = builder.build();
		ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
		return new ElasticsearchClient(transport);
	}
}