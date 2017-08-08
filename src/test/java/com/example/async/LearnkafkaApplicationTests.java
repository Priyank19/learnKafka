package com.example.async;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import com.example.async.kafka.Receiver;
import com.example.async.kafka.Sender;
import com.example.async.model.Car;

@RunWith(SpringRunner.class)
@SpringBootTest
public class LearnkafkaApplicationTests {

	@Autowired
	private Sender sender;
	@Autowired
	private Receiver receiver;

	@Autowired
	private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

	// Comment in case of instance running.
	// This goes as topic names to be created
	private static String HELLOWORLD_TOPIC = "helloworld.t";
	private static String CAR_TOPIC = "json.t";
	private static String PARTITIONED_TOPIC = "partition.t";

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, 2, HELLOWORLD_TOPIC, CAR_TOPIC,
			PARTITIONED_TOPIC);

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		System.setProperty("kafka.bootstrapAddress", embeddedKafka.getBrokersAsString());
	}

	@Before
	public void setUp() throws Exception {
		// Wait till the partitions are assigned!
		for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry
				.getListenerContainers()) {
			ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafka.getPartitionsPerTopic());
		}
	}

	@Ignore
	public void testReceive() throws Exception {
		sender.send("Hello Spring Kafka!");
		sender.send("Hello World Kafka!");
		sender.send("Hello world Spring!");

		receiver.getFilteredLatch().await(10, TimeUnit.SECONDS);
		assertThat(receiver.getFilteredLatch().getCount()).isEqualTo(0);

		receiver.getLatch().await(10, TimeUnit.SECONDS);
		assertThat(receiver.getLatch().getCount()).isEqualTo(0);
	}

	@Ignore
	public void testJsonReceive() throws Exception {
		sender.send(new Car("Passat", "Volkswagen", "ABC-123"), -1);
		receiver.getJsonLatch().await(10, TimeUnit.SECONDS);
		assertThat(receiver.getJsonLatch().getCount()).isEqualTo(0);
	}

	@Test
	public void testPartitionReceive() throws Exception {
		for (int i = 0; i < 2; i++) {
			sender.send(new Car("amaze", "Honda", "" + i * i), i);
		}
		receiver.getPartitionLatch().await(10, TimeUnit.SECONDS);
		assertThat(receiver.getPartitionLatch().getCount()).isEqualTo(0);
	}

}
