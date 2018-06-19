package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class PartitionedProducer {
  public static void main(String[] args){
    if (args.length != 1) {
      System.out.println("Please provide command line arguments: numEvents");
      System.exit(-1);
    }
    long events = Long.parseLong(args[0]);

    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("partitioner.class", "producer.RangePartitioner");

    String topic = "PartitionedProducer";
    
    KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
    ProducerRecord<Integer, String> producerRecord = null;
    
    for (int cEvent = 0; cEvent < events; cEvent++) {
    	producerRecord = new ProducerRecord<Integer, String>(topic, new Integer(cEvent), "This is message no. " + cEvent);
    	producer.send(producerRecord);
    	System.out.println("Record Sent!");
    }
    producer.close();
  } 
}
