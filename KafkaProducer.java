package com.btmnr.producer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {

	private static Producer<Integer, String> producer;
	private static final String topic = "mytopic";

	public void initialize() {
		Properties producerProps = new Properties();
		producerProps.put("metadata.broker.list", "10.35.54.217:9092");
		producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
		producerProps.put("request.required.acks", "1");
		ProducerConfig producerConfig = new ProducerConfig(producerProps);
		producer = new Producer<Integer, String>(producerConfig);
		System.out.println("initialized");
	}

	public void publishMesssage() throws Exception {
		System.out.println("Publishing msg");
		String fileName = "C:/tempFolder/generic_traps.log";

		try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {

			String line;
			while ((line = br.readLine()) != null) {
				KeyedMessage<Integer, String> keyedMsg = new KeyedMessage<Integer, String>(topic, line);
				producer.send(keyedMsg); // This publishes message on given topic
				System.out.println("--> Message [" + line + "] sent. Check message on Consumer's program console");
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) throws Exception {
		KafkaProducer kafkaProducer = new KafkaProducer();
		// Initialize producer
		kafkaProducer.initialize();
		// Publish message
		kafkaProducer.publishMesssage();
		// Close the producer
		producer.close();
	}

}