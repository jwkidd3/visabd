package com.kiddcorp.kafka.clients;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class TheProducer {

	public static void main(String[] args) throws Exception {
		long numEvts = 10;

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", StringSerializer.class.getName());
		props.put("value.serializer", StringSerializer.class.getName());
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);

		KafkaProducer<String, String> producer = new KafkaProducer<>(props);
		/*
		 * For console consumer use: kafka-console-consumer.sh
		 * --bootstrap-server localhost:9092 --topic testout --from-beginning
		 * --property key.deserializer=org.apache.kafka.common.serialization.
		 * StringDeserializer --property
		 * value.deserializer=org.apache.kafka.common.serialization.
		 * IntgerDeserializer
		 */
		while (1 == 1) {
			for (int i = 0; i < numEvts; i++) {
				String key = "bubba" + i;
				String val = "New Value: " + i;
				ProducerRecord<String, String> rec = new ProducerRecord<>("testTopic", key, val);
				producer.send(rec);
			}
			Thread.sleep(5000);
		}
		//producer.close();
	}

}
