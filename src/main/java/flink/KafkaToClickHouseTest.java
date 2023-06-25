package flink;

import static flink.ApplicationConstant.CLICKHOUSE_URL;
import static flink.ApplicationConstant.DATE_TIME_FORMATTER;
import static flink.ApplicationConstant.CLICKHOUSE_TABLE;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaToClickHouseTest {

	public static void main(String[] args) {
		String JSON = "{\"id\":1,\"name\":\"lenovo thinkbook 14 core i3 16gb\",\"description\":\"lenovo thinkbook laptop\",\"price\":22000.0,\"category\":\"laptop\"}";
		System.out.println(JSON);
		HashMap<String, Object> mapData = new HashMap<>();
		try {
			// insertIntoClickHouse(JSON);

			Set<String> dbColumns = readFromClickHouse("SELECT name FROM system.columns WHERE table = '" + CLICKHOUSE_TABLE + "'");
			log.info("dbColumns = {}", dbColumns);
			mapData = new ObjectMapper().readValue(JSON, new TypeReference<HashMap<String, String>>() {
			});
			log.info("mapData.keySet = {}", mapData.keySet());
			if (mapData.keySet().containsAll(dbColumns)) {
				log.info("=====================if");
			} else {
				log.info("=====================else");
			}
		} catch (Exception e) {
			log.error(":: Exception occured :: {}", e);
		}
	}

	/**
	 * processAndInsertIntoClickHouse()
	 * 
	 * @param connection
	 * @param message
	 * @throws SQLException
	 */
	public static void insertIntoClickHouse(String message) throws SQLException {

		// SQL Query
		String INSERT_QUERY = "INSERT INTO " + CLICKHOUSE_TABLE + " VALUES (generateUUIDv4(), ?, ?, ?)";
		
		try (Connection connection = DriverManager.getConnection(CLICKHOUSE_URL);
				PreparedStatement statement = connection.prepareStatement(INSERT_QUERY);) {

			// Get current LocalDateTime & Format LocalDateTime to String
			String formattedDateTime = LocalDateTime.now().format(DATE_TIME_FORMATTER);

			statement.setString(1, message);
			statement.setString(2, message);
			statement.setString(3, formattedDateTime);

			statement.executeUpdate();

		} catch (SQLException e) {
			log.error(":: SQLException occured :: {}", e);
		}
	}

	/**
	 * processAndInsertIntoClickHouse()
	 * 
	 * @param connection
	 * @param message
	 * @throws SQLException
	 */
	public static Set<String> readFromClickHouse(String SQL) throws SQLException {

		Set<String> columns = new HashSet<>();
		try (Connection connection = DriverManager.getConnection(CLICKHOUSE_URL);
				PreparedStatement statement = connection.prepareStatement(SQL);
				ResultSet rs = statement.executeQuery();) {

			if (log.isInfoEnabled())
				log.info("Formatted LocalDateTime in String format : {}", SQL);

			while (rs.next()) {
				columns.add(rs.getString(1));
			}
			log.info("===============>{}", columns);
		} catch (SQLException e) {
			log.error(":: SQLException :: {}", e);
		}
		return columns;
	}

}
