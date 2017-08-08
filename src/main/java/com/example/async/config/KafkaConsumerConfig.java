package com.example.async.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import com.example.async.model.Car;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

	@Value(value = "${kafka.bootstrapAddress}")
	private String bootstrapAddress;

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
		// allows a pool of processes to divide the work of consuming and
		// processing records
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "helloworld");
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(new DefaultKafkaConsumerFactory<String, String>(props, new StringDeserializer(),
				new StringDeserializer()));
		return factory;
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, Car> jsonKafkaListenerContainerFactory() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "json");

		ConcurrentKafkaListenerContainerFactory<String, Car> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(
				new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new JsonDeserializer<>(Car.class)));
		return factory;
	}

	// Record Filter example.
	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> filterKafkaListenerContainerFactory() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "filter");

		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(
				new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new StringDeserializer()));
		// Prevent World being delivered!
		factory.setRecordFilterStrategy(record -> record.value().contains("World"));
		return factory;
	}

}
