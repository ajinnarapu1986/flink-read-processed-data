package flink;

import static flink.ApplicationConstant.DB_TABLE;
import static flink.ApplicationConstant.CLICKHOUSE_URL;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaToClickHouseTest {

	public static void main(String[] args) {
		String JSON = "{\"id\":1,\"name\":\"lenovo thinkbook 14 core i3 16gb\",\"description\":\"lenovo thinkbook laptop\",\"price\":22000.0,\"category\":\"laptop\"}";
		System.out.println(JSON);
		Map<String, Object> data = new LinkedHashMap<>();

		try {
			data = new ObjectMapper().readValue(JSON, new TypeReference<HashMap<String, String>>() {
			});
			log.info("mapData.keySet = {}", data);

			executeInsertQuery(data);
		} catch (Exception e) {
			log.error(":: Exception occured :: {}", e);
		}
	}

	public static void executeInsertQuery(Map<String, Object> data) {
		// SQL query placeholders
		StringBuilder columns = new StringBuilder();
		StringBuilder values = new StringBuilder();

		for (String key : data.keySet()) {
			columns.append(key).append(", ");
			values.append("?, ");
		}

		// Remove the trailing commas
        columns.setLength(columns.length() - 2);
        values.setLength(values.length() - 2);

		// Construct the parameterized SQL INSERT query
		String sqlQuery = "INSERT INTO " + DB_TABLE + " (" + columns + ") VALUES (" + values + ")";
		log.info(sqlQuery);
		try (Connection connection = DriverManager.getConnection(CLICKHOUSE_URL);
				PreparedStatement statement = connection.prepareStatement(sqlQuery)) {

			int parameterIndex = 1;
			for (String key : data.keySet()) {
				Object value = data.get(key);

				if (value instanceof Integer) {
					statement.setInt(parameterIndex, (Integer) value);
				} else if (value instanceof String) {
					statement.setString(parameterIndex, (String) value);
				} else if (value instanceof Double) {
					statement.setDouble(parameterIndex, (Double) value);
				} // Add more data types as needed

				parameterIndex++;
			}

			// Execute the query
			statement.executeUpdate();
			log.info("Data inserted successfully!");

		} catch (SQLException e) {
			log.error("Error executing the query: " + e.getMessage());
		}
	}

}
