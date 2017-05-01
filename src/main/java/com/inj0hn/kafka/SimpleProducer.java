package com.inj0hn.kafka;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

public class SimpleProducer {
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void main(String[] args) throws Exception{
		String topic = System.getProperty("topic");
		String configFile = System.getProperty("configFile");
		String message = System.getProperty("message");
		
		System.out.println("Topic: " + topic);
		System.out.println("Config file: " + configFile);
		System.out.println("Message: " + message);
		Properties props = PropertiesLoaderUtils
				.loadProperties(new FileSystemResource(configFile));
		sendMessage(topic, (Map)props, message);
	}

	private static <T> void sendMessage(String topic, Map<String, Object> producerProps, T data) {
		KafkaProducer<Integer, T> producer = new KafkaProducer<Integer, T>(producerProps);
		try{
			ProducerRecord<Integer, T> message = new ProducerRecord<>(topic, data);
			producer.send(message);
		}finally{
			producer.close();
		}
	}
}
