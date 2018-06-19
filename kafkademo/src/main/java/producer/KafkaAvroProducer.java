package producer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaAvroProducer {
	public static void main(String[] args) throws InterruptedException, ExecutionException, FileNotFoundException, IOException {
		if(args.length != 2) {
			System.out.println("Pass two arguments");
			System.exit(-1);
		}
		
		String fileName = args[0];
		String delimiter = args[1];
		
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
		          io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
		          io.confluent.kafka.serializers.KafkaAvroSerializer.class);
		
		//Provide URI of the same Schema Registry
		props.put("schema.registry.url", "http://localhost:8081");
		KafkaProducer producer = new KafkaProducer(props);
		
		//We provide the Avro schema, since it is not provided by the Avro generated object

		//name of keySchema and valueSchema must be different.
		String keySchema = "{\"type\":\"record\"," +
                "\"name\":\"keyrecord\"," +
                "\"fields\":[{\"name\":\"eid\",\"type\":\"string\"},"
                + "{\"name\":\"ename\",\"type\":\"string\"}]}";

		
		String valueSchema = "{\"type\":\"record\"," +
		                    "\"name\":\"valuerecord\"," +
		                    "\"fields\":[{\"name\":\"dept\",\"type\":\"string\"},"
		                    + "{\"name\":\"salary\",\"type\":\"int\"}]}";
		
		String topic = "KafkaAvroTopic";
		
		try(BufferedReader br = new BufferedReader(new FileReader(fileName))) {
	        for(String line; (line = br.readLine()) != null; ) {
	            String[] tempArray = line.split(delimiter);
	            String eid = tempArray[0];
	            String ename = tempArray[1];
	            String dept = tempArray[2];
	            int salary = Integer.parseInt(tempArray[3]);

	    		Schema.Parser parser = new Schema.Parser();
	    		Schema schema = parser.parse(keySchema);
	    		GenericRecord avroKey = new GenericData.Record(schema);
	    		
	    		avroKey.put("eid", eid);
	    		avroKey.put("ename", ename);

	    		schema = parser.parse(valueSchema);
	    		GenericRecord avroRecord = new GenericData.Record(schema);
	    		
	    		avroRecord.put("dept", dept);		
	    		avroRecord.put("salary", salary);
	    		
	    		ProducerRecord record = new ProducerRecord<Object, Object>(topic, avroKey, avroRecord);
	        	producer.send(record).get();
	           	System.out.printf("Record sent to topic:%s and acknowledged as well. Key:%s, Value:%s\n", topic, avroKey, avroRecord);
	           	}
	    }
	}
}
