package producer;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

public class RangePartitioner implements Partitioner {
		public void configure(Map<String, ?> configs) {}
		
		public int partition(String topic, Object key, byte[] keyBytes,
			Object value, byte[] valueBytes, Cluster cluster) {
				List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
				
				// Getting the number of partitions for the topic
				int numPartitions = partitions.size();
				if ((Integer) key < 1000) {
					System.out.println("The key: " + key + " will go to partition: " + (numPartitions - 1));
					return numPartitions - 2; 
				}
				System.out.println("The key: " + key + " will go to partition: " + numPartitions);
				return numPartitions - 1;
			}
		
		public void close() {}
	}
