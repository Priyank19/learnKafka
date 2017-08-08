package com.example.async.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.example.async.model.Car;

@Configuration
public class KafkaProducerConfig {

	// list of host:port pairs.
	@Value(value = "${kafka.bootstrapAddress}")
	private String bootstrapAddress;

	// Step 1: Configure a ProducerFactory which sets the strategy for creating
	// Kafka Producer instances.
	@Bean
	public ProducerFactory<String, String> producerFactory() {
		Map<String, Object> configProps = new HashMap<>();
		configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
	
		configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		return new DefaultKafkaProducerFactory<>(configProps);
	}

	// Step 2: A KafkaTemplate wraps a Producer instance and provides
	// convenience methods for sending msgs to topics - needs ProducerFactory
	// The template provides both asynchronous and synchronous send methods,
	// with the asynchronous methods returning a Future
	@Bean
	public KafkaTemplate<String, String> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}

	// JSON Objects
	@Bean
	public ProducerFactory<String, Car> jsonProducerFactory() {
		Map<String, Object> configProps = new HashMap<>();
		configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
		configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		return new DefaultKafkaProducerFactory<>(configProps);
	}
	@Bean
	public KafkaTemplate<String, Car> jsonKafkaTemplate() {
		return new KafkaTemplate<>(jsonProducerFactory());
	}}
