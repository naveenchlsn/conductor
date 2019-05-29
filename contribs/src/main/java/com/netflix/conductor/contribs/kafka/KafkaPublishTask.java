package com.netflix.conductor.contribs.kafka;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@Singleton
public class KafkaPublishTask extends WorkflowSystemTask {

	public static final String REQUEST_PARAMETER_NAME = "kafka_request";
	public static final String NAME = "KAFKA_PUBLISH";
	static final String MISSING_REQUEST = "Missing Kafka request. Task input MUST have a '" + REQUEST_PARAMETER_NAME + "' key with KafkaTask.Input as value. See documentation for KafkaTask for required input parameters";
	private static final Logger logger = LoggerFactory.getLogger(KafkaPublishTask.class);
	public static final String NO_BOOT_STRAP_SERVERS_SPECIFIED = "No boot strap servers specified";
	public static final String MISSING_KAFKA_TOPIC = "Missing Kafka topic. See documentation for KafkaTask for required input parameters";
	public static final String MISSING_KAFKA_BODY = "Missing Kafka body.  See documentation for KafkaTask for required input parameters";
	public static final String FAILED_TO_INVOKE = "Failed to invoke kafka task due to: ";
	protected ObjectMapper om = objectMapper();
	protected Configuration config;
	private String requestParameter;
	KafkaProducerManager producerManager;


	@Inject
	public KafkaPublishTask(Configuration config, KafkaProducerManager clientManager) {
		super(NAME);
		this.config = config;
		this.requestParameter = REQUEST_PARAMETER_NAME;
		this.producerManager = clientManager;
		logger.info("KafkaTask initialized...");
	}

	private static ObjectMapper objectMapper() {
		final ObjectMapper om = new ObjectMapper();
		om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		om.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
		om.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
		om.setSerializationInclusion(JsonInclude.Include.NON_NULL);
		om.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
		return om;
	}

	@Override
	public void start(Workflow workflow, Task task, WorkflowExecutor executor) {

		task.setWorkerId(config.getServerId());
		Object request = task.getInputData().get(requestParameter);

		if (Objects.isNull(request)) {
			task.setReasonForIncompletion(MISSING_REQUEST);
			task.setStatus(Task.Status.FAILED);
			return;
		}

		KafkaPublishTask.Input input = om.convertValue(request, KafkaPublishTask.Input.class);


		if (StringUtils.isBlank(input.bootStrapServers)) {
			task.setReasonForIncompletion(NO_BOOT_STRAP_SERVERS_SPECIFIED);
			task.setStatus(Task.Status.FAILED);
			return;
		}

		if (StringUtils.isBlank(input.getTopic())) {
			task.setReasonForIncompletion(MISSING_KAFKA_TOPIC);
			task.setStatus(Task.Status.FAILED);
			return;
		}

		if (Objects.isNull(input.getValue())) {
			task.setReasonForIncompletion(MISSING_KAFKA_BODY);
			task.setStatus(Task.Status.FAILED);
			return;
		}

		try {

			Future<RecordMetadata> recordMetaDataFuture = kafkaPublish(input);
			try {
				recordMetaDataFuture.get();
				task.setStatus(Task.Status.COMPLETED);
			} catch (ExecutionException ec) {
				task.setStatus(Task.Status.FAILED);
				task.setReasonForIncompletion(ec.getMessage());
			}
			logger.info("Published message {} ", input);

		} catch (Exception e) {
			logger.error(String.format("Failed to invoke kafka task - ", input), e);
			task.setStatus(Task.Status.FAILED);
			task.setReasonForIncompletion(FAILED_TO_INVOKE + e.getMessage());
		}
	}

	/**
	 * @param input Kafka Request
	 * @return Future for execution.
	 * @throws Exception If there was an error making kafka publish
	 */
	public Future<RecordMetadata> kafkaPublish(KafkaPublishTask.Input input) throws Exception {

		long startEpoch = Instant.now().toEpochMilli();

		Producer producer = producerManager.getProducer(input);

		logger.info("Time taken getting producer {}", Instant.now().toEpochMilli() - startEpoch);

		Iterable<Header> headers = input.headers.entrySet().stream().map(h -> new RecordHeader(h.getKey(), String.valueOf(h.getValue()).getBytes())).collect(Collectors.toList());
		ProducerRecord rec = new ProducerRecord(input.topic, null, null, input.key, om.writeValueAsString(input.value), headers);

		Future send = producer.send(rec);
		producer.close();

		return send;
	}

	@Override
	public boolean execute(Workflow workflow, Task task, WorkflowExecutor executor) {
		return false;
	}

	@Override
	public void cancel(Workflow workflow, Task task, WorkflowExecutor executor) {
		task.setStatus(Task.Status.CANCELED);
	}

	@Override
	public boolean isAsync() {
		return true;
	}

	@Override
	public int getRetryTimeInSecond() {
		return 60;
	}

	public static class Input {

		public static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
		private Map<String, Object> headers = new HashMap<>();

		private String bootStrapServers;

		private Object key;

		private Object value;

		private Integer requestTimeoutMs;

		private String topic;

		private String keySerializer = STRING_SERIALIZER;

		public Map<String, Object> getHeaders() {
			return headers;
		}

		public void setHeaders(Map<String, Object> headers) {
			this.headers = headers;
		}

		public String getBootStrapServers() {
			return bootStrapServers;
		}

		public void setBootStrapServers(String bootStrapServers) {
			this.bootStrapServers = bootStrapServers;
		}

		public Object getKey() {
			return key;
		}

		public void setKey(Object key) {
			this.key = key;
		}

		public Object getValue() {
			return value;
		}

		public void setValue(Object value) {
			this.value = value;
		}

		public Integer getRequestTimeoutMs() {
			return requestTimeoutMs;
		}

		public void setRequestTimeoutMs(Integer requestTimeoutMs) {
			this.requestTimeoutMs = requestTimeoutMs;
		}

		public String getTopic() {
			return topic;
		}

		public void setTopic(String topic) {
			this.topic = topic;
		}

		public String getKeySerializer() {
			return keySerializer;
		}

		public void setKeySerializer(String keySerializer) {
			this.keySerializer = keySerializer;
		}

		@Override
		public String toString() {
			return "Input{" +
					"headers=" + headers +
					", bootStrapServers='" + bootStrapServers + '\'' +
					", value=" + value +
					", requestTimeoutMs=" + requestTimeoutMs +
					", topic='" + topic + '\'' +
					", keySerializer='" + keySerializer + '\'' +
					'}';
		}
	}
}
