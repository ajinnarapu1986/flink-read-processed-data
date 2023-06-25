package flink;

import org.apache.flink.api.common.functions.MapFunction;

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
		
		//#############################Processing Logic
		
		KafkaToClickHouseTest.insertIntoClickHouse(value);
		
		//#############################Processing Logic
		
		return value.toLowerCase();
	}

}
