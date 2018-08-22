package com.huoshan.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Description : 连接kafka  生产者
 * Created by ln on : 2018/8/3 09:26
 * Author : ln56
 */
public class OrdProducer {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "ln1:9092,ln2:9092,ln3:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		
		for (int i = 0; i < 3000; i++)
			producer.send(new ProducerRecord<String, String>("test",
					Integer.toString(i), "wudalang"+i));

		producer.close();
	}
}
