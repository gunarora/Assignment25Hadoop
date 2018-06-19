package consumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class TopicsConsumer {
	
	public static void main(String[] args) {
		if(args.length != 2) {
			System.out.println("Provide 2 arguments");
			System.exit(-1);
		}
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "TopicsConsumer");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		
		System.out.println("Starting the consumer");
				
		Collection<String> topics = new ArrayList<String>();
		for(String topic: args[0].split(",")) {
			topics.add(topic);
		}
		
		consumer.subscribe(topics);
		int pollInterval = Integer.parseInt(args[1]);
		
		System.out.println("Topics to consume: " + topics);
		System.out.println("Poll Interval: " + pollInterval);
		
		try {
		
			while (true) {
				System.out.print(".");
				ConsumerRecords<String, String> records = consumer.poll(pollInterval);
				for (ConsumerRecord<String, String> record : records)
				{
					System.out.printf("topic = %s, partition = %s, offset = %s, key = %s, value = %s\n",
					record.topic(), record.partition(), record.offset(), record.key(),
					record.value());
				}
			}
		}
		
		finally {
			consumer.close();
		}
	}
}
