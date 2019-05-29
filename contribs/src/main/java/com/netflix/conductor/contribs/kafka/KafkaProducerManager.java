package com.netflix.conductor.contribs.kafka;

import com.netflix.conductor.contribs.http.HttpTask;
import com.netflix.conductor.core.config.Configuration;
import com.sun.jersey.api.client.Client;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;

import java.time.Instant;
import java.util.Properties;

public class KafkaProducerManager {

	public static final String KAFKA_PUBLISH_REQUEST_TIMEOUT_MS = "kafka.publish.request.timeout.ms";
	public static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
	public int defaultRequestTimeOut = 100;

	public KafkaProducerManager(Configuration configuration) {
		this.defaultRequestTimeOut = configuration.getIntProperty(KAFKA_PUBLISH_REQUEST_TIMEOUT_MS, defaultRequestTimeOut);
	}


	public Producer getProducer(KafkaPublishTask.Input input) {

		Properties configProperties = new Properties();

		configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, input.getBootStrapServers());

		Object key = null;

		if (input.getKeySerializer().equals(LongSerializer.class.getName())) {
			key = Long.parseLong(String.valueOf(input.getKey()));
		} else if (input.getKeySerializer().equals(Integer.class.getName())) {
			key = Integer.parseInt(String.valueOf(input.getKey()));
		} else {
			key = String.valueOf(input.getKey());
		}

		configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, input.getKeySerializer());

		Integer requestTimeoutMs = input.getRequestTimeoutMs();

		if (requestTimeoutMs == null) {
			requestTimeoutMs = defaultRequestTimeOut;
		}

		configProperties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
		configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER);


		Producer producer = new KafkaProducer<String, String>(configProperties);
		return producer;
	}
}
