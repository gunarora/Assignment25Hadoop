package consumer;

import java.util.Collections;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


public class KafkaAvroConsumer {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaAvroConsumer");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
		          io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
		          io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
		props.put("schema.registry.url", "http://localhost:8081");
		
		String topic = "KafkaAvroTopic";
	
		KafkaConsumer consumer = new KafkaConsumer(props);

		consumer.subscribe(Collections.singletonList(topic));
		System.out.println("Reading topic:" + topic);
		while (true) {
				ConsumerRecords<GenericRecord, GenericRecord> records = consumer.poll(1000);
				for (ConsumerRecord<GenericRecord, GenericRecord> record: records) {
					System.out.println("eid from key: " + record.key().get("eid"));
					System.out.println("ename from key: " + record.key().get("ename"));
					System.out.println("department from value: " + record.value().get("dept"));
					System.out.println("salary from value: " + record.value().get("salary"));
				}
		}
	}
}
