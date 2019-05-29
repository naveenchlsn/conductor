package com.netflix.conductor.contribs.kafka;


import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.contribs.http.HttpTask;
import com.netflix.conductor.core.config.SystemPropertiesConfiguration;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.SystemTaskWorkerCoordinator;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

public class TestKafkaPublishTask {

	@Test
	public void missingRequest_Fail(){
		KafkaPublishTask kPublishTask = new KafkaPublishTask(new SystemPropertiesConfiguration(),new KafkaProducerManager(new SystemPropertiesConfiguration()));
		Task task = new Task();
		kPublishTask.start(Mockito.mock(Workflow.class), task,Mockito.mock(WorkflowExecutor.class));
		Assert.assertEquals(Task.Status.FAILED, task.getStatus());
	}

	@Test
	public void missingValue_Fail(){

		Task task = new Task();
		KafkaPublishTask.Input input = new KafkaPublishTask.Input();
		input.setBootStrapServers("localhost:9092");
		Map<String, Object> value = new HashMap<>();
		value.put("input_key1", "value1");
		value.put("input_key2", 45.3d);
		input.setTopic("testTopic");

		//input.setValue(value);
		task.getInputData().put(KafkaPublishTask.REQUEST_PARAMETER_NAME, input);

		KafkaPublishTask kPublishTask = new KafkaPublishTask(new SystemPropertiesConfiguration(), new KafkaProducerManager(new SystemPropertiesConfiguration()));
		kPublishTask.start(Mockito.mock(Workflow.class), task,Mockito.mock(WorkflowExecutor.class));
		Assert.assertEquals(Task.Status.FAILED, task.getStatus());
	}

	@Test
	public void missingBootStrapServers_Fail(){

		Task task = new Task();
		KafkaPublishTask.Input input = new KafkaPublishTask.Input();
		//input.setBootStrapServers("localhost:9092");

		Map<String, Object> value = new HashMap<>();

		value.put("input_key1", "value1");
		value.put("input_key2", 45.3d);

		input.setValue(value);
		input.setTopic("testTopic");

		task.getInputData().put(KafkaPublishTask.REQUEST_PARAMETER_NAME, input);

		KafkaPublishTask kPublishTask = new KafkaPublishTask(new SystemPropertiesConfiguration(),new KafkaProducerManager(new SystemPropertiesConfiguration()));
		kPublishTask.start(Mockito.mock(Workflow.class), task,Mockito.mock(WorkflowExecutor.class));
		Assert.assertEquals(Task.Status.FAILED, task.getStatus());
	}


	@Test
	public void kafkaPublishException_Fail() throws ExecutionException, InterruptedException {

		Task task = new Task();
		KafkaPublishTask.Input input = new KafkaPublishTask.Input();
		input.setBootStrapServers("localhost:9092");

		Map<String, Object> value = new HashMap<>();

		value.put("input_key1", "value1");
		value.put("input_key2", 45.3d);

		input.setValue(value);
		input.setTopic("testTopic");
		task.getInputData().put(KafkaPublishTask.REQUEST_PARAMETER_NAME, input);

		KafkaProducerManager producerManager = Mockito.mock(KafkaProducerManager.class);
		KafkaPublishTask kPublishTask = new KafkaPublishTask(new SystemPropertiesConfiguration(), producerManager);

		Producer producer = Mockito.mock(Producer.class);

		Mockito.when(producerManager.getProducer(Mockito.any())).thenReturn(producer);
		Future publishingFuture = Mockito.mock(Future.class);
		Mockito.when(producer.send(Mockito.any())).thenReturn(publishingFuture);

		ExecutionException executionException = Mockito.mock(ExecutionException.class);


		Mockito.when(executionException.getMessage()).thenReturn("Execution exception");
		Mockito.when(publishingFuture.get()).thenThrow(executionException);

		kPublishTask.start(Mockito.mock(Workflow.class), task,Mockito.mock(WorkflowExecutor.class));
		Assert.assertEquals(Task.Status.FAILED, task.getStatus());
		Assert.assertEquals("Execution exception", task.getReasonForIncompletion());
	}
}
