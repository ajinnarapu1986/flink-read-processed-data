package flink;

import java.time.format.DateTimeFormatter;

public interface ApplicationConstant {

	public static final String KAFKA_TOPIC = "processed-data";
	public static final String KAFKA_SERVER = "localhost:9092";
	public static final String KAFKA_GROUP_ID = "PROCESSED-DATA";

	public static final String CLICKHOUSE_TABLE = "log_data";
	public static final String CLICKHOUSE_URL = "jdbc:clickhouse://localhost:8123/default";

	// Create DateTimeFormatter instance with specified format
	public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
}
