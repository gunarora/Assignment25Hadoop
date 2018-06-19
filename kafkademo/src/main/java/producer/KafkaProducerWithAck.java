package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerWithAck {
  public static void main(String[] args) throws IOException, InterruptedException, ExecutionException{
    if (args.length != 2) {
      System.out.println("Please provide appropriate command line arguments");
      System.exit(-1);
    }

    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("retries", 3);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    KafkaProducer<String, String> producer = new KafkaProducer<>(props);
    ProducerRecord<String, String> producerRecord = null;

    String fileName = args[0];
    String delimiter = args[1];
    
    try(BufferedReader br = new BufferedReader(new FileReader(fileName))) {
        for(String line; (line = br.readLine()) != null; ) {
            String[] tempArray = line.split(delimiter);
            String topic = tempArray[0];
            String key = tempArray[1];
            String value = tempArray[2];

            producerRecord = new ProducerRecord<String, String>(topic, key, value);
        	producer.send(producerRecord).get();
           	System.out.printf("Record sent to topic:%s and acknowledged as well. Key:%s, Value:%s\n", topic, key, value);
           	}
    }
    producer.close();
  } 
}
