package flink;

import java.util.HashMap;

import org.apache.flink.api.common.functions.MapFunction;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClickHouseConsumer implements MapFunction<String, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 868789200034301194L;

	@Override
	public String map(String value) throws Exception {

		log.info("Pushing to ClickHouse : " + value);

		final ObjectMapper mapper = new ObjectMapper();
		HashMap<String, Object> data = mapper.readValue(value, new TypeReference<HashMap<String, String>>() {
		});
		// #############################Processing Logic

		KafkaToClickHouseTest.executeInsertQuery(data);

		// #############################Processing Logic

		return value.toLowerCase();
	}

}
