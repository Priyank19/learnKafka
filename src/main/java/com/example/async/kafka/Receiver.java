package com.example.async.kafka;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.example.async.model.Car;

@Component
public class Receiver {

	private static final Logger LOGGER = LoggerFactory.getLogger(Receiver.class);
	// Latch is just for testing.
	private CountDownLatch latch = new CountDownLatch(3);
	private CountDownLatch jsonLatch = new CountDownLatch(1);
	private CountDownLatch filteredLatch = new CountDownLatch(1);
	private CountDownLatch partitionLatch = new CountDownLatch(2);

	public CountDownLatch getLatch() {
		return latch;
	}

	public CountDownLatch getJsonLatch() {
		return jsonLatch;
	}

	public CountDownLatch getFilteredLatch() {
		return filteredLatch;
	}

	public CountDownLatch getPartitionLatch() {
		return partitionLatch;
	}

	// Requires a setup of kafkaListenerContainerFactory & @EnableKafka
	// Another @KafkaListner with same containerGroup will consume in parallel
	// if and only if there are more than than 1 partition.
	// Even we can have multiple topics as comma serperated values for a single
	// KafkaListener e.g. topics = "topic1, topic2", containerGroup = "foo"
	@KafkaListener(topics = "${kafka.topic.helloworld}", containerGroup = "listnerGrp1", containerFactory = "kafkaListenerContainerFactory")
	public void receive(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
		LOGGER.info("received message='{}' on partition {}", message, partition);
		latch.countDown();
	}

	@KafkaListener(topics = "${kafka.topic.helloworld}", containerFactory = "filterKafkaListenerContainerFactory")
	public void receiveFiltered(String message) {
		LOGGER.info("received filtered message='{}'", message);
		filteredLatch.countDown();
	}

	@KafkaListener(topics = "${kafka.topic.json}", containerFactory = "jsonKafkaListenerContainerFactory")
	public void receive(Car car) {
		LOGGER.info("received json message='{}'", car);
		jsonLatch.countDown();
	}

	// @Payload is optional, @Header can fetch more information
	@KafkaListener(topicPartitions = @TopicPartition(topic = "${kafka.topic.partitioned}", partitions = { "0",
			"1" }), containerFactory = "jsonKafkaListenerContainerFactory")
	public void listenToParition(@Payload Car car, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
			@Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
		LOGGER.info("received Car {} from {}@{}", car, topic, partition);
		partitionLatch.countDown();
	}
}
