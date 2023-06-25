package flink;

import static flink.ApplicationConstant.KAFKA_TOPIC;
import static flink.ApplicationConstant.KAFKA_SERVER;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PushToClickHouseMain {

	

	public static void main(String[] args) throws Exception {
		log.info(":: insiode main() ::");
		StreamProcessedDataConsumrer(KAFKA_TOPIC, KAFKA_SERVER);
	}

	/**
	 * 
	 * @param inputTopic
	 * @param server
	 * @throws Exception
	 */
	public static void StreamProcessedDataConsumrer(String inputTopic, String server) throws Exception {
		log.info(":: insiode StreamProcessedDataConsumrer() ::");
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
				
		FlinkKafkaConsumer011<String> flinkKafkaConsumer = createStringConsumerForTopic(inputTopic, server);
		DataStream<String> stringInputStream = environment.addSource(flinkKafkaConsumer);
		
		log.info(":: Writing dat to the Click House DB : Start ::");
		stringInputStream.map(new ClickHouseConsumer());
		log.info(":: Writing dat to the Click House DB : Completed ::");

		environment.execute();
		log.info(":: Exiting StreamProcessedDataConsumrer() ::");
		
	}

	/**
	 * 
	 * @param topic
	 * @param kafkaAddress
	 * @return
	 */
	public static FlinkKafkaConsumer011<String> createStringConsumerForTopic(String topic, String kafkaAddress) {
		log.info(":: insiode createStringConsumerForTopic() ::");
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", kafkaAddress);
		// props.setProperty("group.id",kafkaGroup);
		FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), props);

		return consumer;
	}
}
