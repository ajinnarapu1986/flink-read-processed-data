package flink;

import java.util.HashMap;

import org.apache.flink.api.common.functions.MapFunction;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClickHouseConsumer implements MapFunction<String, String> {
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 868789200034301194L;

	@SuppressWarnings("unchecked")
	@Override
	public String map(String value) throws Exception {
		
		
		log.info("Pushing to ClickHouse : " + value);
		
		ObjectMapper mapper = new ObjectMapper();
		HashMap<String, Object> mapData = mapper.readValue(value, HashMap.class);
		System.out.println(mapData.keySet());
		
		//#############################Processing Logic
		
		KafkaToClickHouseTest.insertIntoClickHouse(value);
		
		//#############################Processing Logic
		
		return value.toLowerCase();
	}

}
