package org.example.datalake;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataLakeToDatabaseMigrator {
    private static final Logger logger = LoggerFactory.getLogger(DataLakeToDatabaseMigrator.class);

    private static final String DB_PATH = "src/main/DataLake/DataMarkt/bitFOMO.db";
    private static final String BINANCE_DATA_DIR = "src/main/DataLake/DataLake/bitcoin_data";
    private static final String REDDIT_DATA_DIR = "src/main/DataLake/DataLake/reddit_data";
    private static final SimpleDateFormat BINANCE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    private static final SimpleDateFormat REDDIT_DATE_FORMAT = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");

    public void migrateDataLakeToDatabase() {
        try {
            logger.info("Iniciando migración de datos del DataLake a la base de datos...");

            logger.info("Migrando datos de Binance...");
            migrateBinanceData();

            logger.info("Migrando datos de Reddit...");
            migrateRedditData();

            logger.info("Migración completada exitosamente.");
        } catch (Exception e) {
            logger.error("Error durante la migración: {}", e.getMessage(), e);
            throw new RuntimeException("Fallo en la migración del DataLake a la base de datos", e);
        }
    }

    private void migrateBinanceData() throws Exception {
        File dir = new File(BINANCE_DATA_DIR);
        if (!dir.exists() || !dir.isDirectory()) {
            logger.warn("Directorio de datos de Binance no encontrado: {}", BINANCE_DATA_DIR);
            return;
        }

        List<Map<String, Object>> dataList = new ArrayList<>();
        for (File file : dir.listFiles((d, name) -> name.endsWith(".txt"))) {
            logger.info("Procesando archivo de Binance: {}", file.getName());
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line;
                Map<String, Object> data = null;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;

                    if (line.startsWith("Bitcoin Price Data") || line.startsWith("Bitcoin Real-time Data")) {
                        continue;
                    }
                    if (line.startsWith("Interval:")) {
                        continue;
                    }
                    if (line.startsWith("------------------------------------")) {
                        if (data != null) {
                            logger.debug("Datos parseados del archivo {}: {}", file.getName(), data);
                            dataList.add(data);
                        }
                        data = new HashMap<>();
                        continue;
                    }
                    if (data == null) continue;

                    if (line.startsWith("Time (UTC):")) {
                        String timeStr = line.substring("Time (UTC):".length()).trim();
                        try {
                            data.put("date", parseBinanceDate(timeStr));
                        } catch (Exception e) {
                            logger.error("Error parseando fecha '{}' en archivo {}: {}", timeStr, file.getName(), e.getMessage());
                            data.put("date", null);
                        }
                    } else if (line.startsWith("Open Price:")) {
                        try {
                            data.put("open_price", Double.parseDouble(line.substring("Open Price:".length()).trim()));
                        } catch (NumberFormatException e) {
                            logger.error("Error parseando Open Price en archivo {}: {}", file.getName(), e.getMessage());
                        }
                    } else if (line.startsWith("High Price:") || line.startsWith("High Price (24h):")) {
                        String prefix = line.contains("High Price (24h):") ? "High Price (24h):" : "High Price:";
                        try {
                            data.put("highest_price_day", Double.parseDouble(line.substring(prefix.length()).trim()));
                        } catch (NumberFormatException e) {
                            logger.error("Error parseando High Price en archivo {}: {}", file.getName(), e.getMessage());
                        }
                    } else if (line.startsWith("Low Price:") || line.startsWith("Low Price (24h):")) {
                        String prefix = line.contains("Low Price (24h):") ? "Low Price (24h):" : "Low Price:";
                        try {
                            data.put("lowest_price_day", Double.parseDouble(line.substring(prefix.length()).trim()));
                        } catch (NumberFormatException e) {
                            logger.error("Error parseando Low Price en archivo {}: {}", file.getName(), e.getMessage());
                        }
                    } else if (line.startsWith("Close Price:")) {
                        try {
                            data.put("current_price", Double.parseDouble(line.substring("Close Price:".length()).trim()));
                        } catch (NumberFormatException e) {
                            logger.error("Error parseando Close Price en archivo {}: {}", file.getName(), e.getMessage());
                        }
                    } else if (line.startsWith("Last Price:")) {
                        try {
                            data.put("current_price", Double.parseDouble(line.substring("Last Price:".length()).trim()));
                        } catch (NumberFormatException e) {
                            logger.error("Error parseando Last Price en archivo {}: {}", file.getName(), e.getMessage());
                        }
                    } else if (line.startsWith("Volume:")) {
                        try {
                            data.put("volume_day", Double.parseDouble(line.substring("Volume:".length()).trim()));
                        } catch (NumberFormatException e) {
                            logger.error("Error parseando Volume en archivo {}: {}", file.getName(), e.getMessage());
                        }
                    } else if (line.startsWith("Price Change %:")) {
                        try {
                            String percentStr = line.substring("Price Change %:".length()).trim().replace("%", "");
                            data.put("price_change_percent", Double.parseDouble(percentStr));
                        } catch (NumberFormatException e) {
                            logger.error("Error parseando Price Change % en archivo {}: {}", file.getName(), e.getMessage());
                        }
                    }
                }
                if (data != null && !data.isEmpty()) {
                    logger.debug("Datos parseados del archivo {}: {}", file.getName(), data);
                    dataList.add(data);
                }
            } catch (Exception e) {
                logger.error("Error al procesar el archivo de Binance {}: {}", file.getName(), e.getMessage(), e);
            }
        }
        saveBinanceData(dataList);
    }

    private void saveBinanceData(List<Map<String, Object>> dataList) throws SQLException {
        if (dataList.isEmpty()) {
            logger.info("No hay datos de Binance para guardar.");
            return;
        }
        String sql = "INSERT OR REPLACE INTO FinantialData (VolumeDay, LowestPriceDay, HighestPriceDay, " +
                "PriceChangePercent, CurrentPrice, Date) VALUES (?, ?, ?, ?, ?, ?)";
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + DB_PATH);
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            int savedRecords = 0;
            int skippedRecords = 0;

            for (Map<String, Object> data : dataList) {
                if (!data.containsKey("volume_day") || data.get("volume_day") == null) {
                    logger.warn("Registro de Binance incompleto: falta 'volume_day'. Registro: {}", data);
                    skippedRecords++;
                    continue;
                }
                if (!data.containsKey("lowest_price_day") || data.get("lowest_price_day") == null) {
                    logger.warn("Registro de Binance incompleto: falta 'lowest_price_day'. Registro: {}", data);
                    skippedRecords++;
                    continue;
                }
                if (!data.containsKey("highest_price_day") || data.get("highest_price_day") == null) {
                    logger.warn("Registro de Binance incompleto: falta 'highest_price_day'. Registro: {}", data);
                    skippedRecords++;
                    continue;
                }
                if (!data.containsKey("current_price") || data.get("current_price") == null) {
                    logger.warn("Registro de Binance incompleto: falta 'current_price'. Registro: {}", data);
                    skippedRecords++;
                    continue;
                }
                if (!data.containsKey("date") || data.get("date") == null) {
                    logger.warn("Registro de Binance incompleto: falta 'date'. Registro: {}", data);
                    skippedRecords++;
                    continue;
                }

                stmt.setDouble(1, (Double) data.get("volume_day"));
                stmt.setDouble(2, (Double) data.get("lowest_price_day"));
                stmt.setDouble(3, (Double) data.get("highest_price_day"));
                setDoubleOrNull(stmt, 4, data.get("price_change_percent"));
                stmt.setDouble(5, (Double) data.get("current_price"));
                stmt.setTimestamp(6, (Timestamp) data.get("date"));

                savedRecords += stmt.executeUpdate();
            }

            logger.info("Se guardaron/actualizaron {} registros de Binance en la base de datos.", savedRecords);
            if (skippedRecords > 0) {
                logger.warn("Se omitieron {} registros de Binance debido a datos incompletos.", skippedRecords);
            }
        }
    }

    private void migrateRedditData() throws Exception {
        File dir = new File(REDDIT_DATA_DIR);
        if (!dir.exists() || !dir.isDirectory()) {
            logger.warn("Directorio de datos de Reddit no encontrado: {}", REDDIT_DATA_DIR);
            return;
        }

        List<Map<String, Object>> dataList = new ArrayList<>();
        for (File file : dir.listFiles((d, name) -> name.endsWith(".txt"))) {
            logger.info("Procesando archivo de Reddit: {}", file.getName());
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line;
                Map<String, Object> data = null;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;
                    if (line.startsWith("------------------------------------------------------------")) {
                        if (data != null) {
                            dataList.add(data);
                        }
                        data = new HashMap<>();
                        continue;
                    }
                    if (data == null) continue;

                    if (line.startsWith("Author:")) {
                        data.put("author", line.substring("Author:".length()).trim());
                    } else if (line.startsWith("Date:")) {
                        String dateStr = line.substring("Date:".length()).trim();
                        try {
                            data.put("date", new Timestamp(REDDIT_DATE_FORMAT.parse(dateStr).getTime()));
                        } catch (Exception e) {
                            logger.error("Error parseando fecha en archivo Reddit {}: {}", file.getName(), e.getMessage());
                            data.put("date", null);
                        }
                    } else if (line.startsWith("URL:")) {
                        data.put("link_to_site", line.substring("URL:".length()).trim());
                    } else if (line.startsWith("Subreddit:")) {
                        data.put("subreddit_name", line.substring("Subreddit:".length()).trim());
                    }
                }
                if (data != null && !data.isEmpty()) {
                    dataList.add(data);
                }
            } catch (Exception e) {
                logger.error("Error al procesar el archivo de Reddit {}: {}", file.getName(), e.getMessage(), e);
            }
        }
        saveRedditData(dataList);
    }

    private void saveRedditData(List<Map<String, Object>> dataList) throws SQLException {
        if (dataList.isEmpty()) {
            logger.info("No hay datos de Reddit para guardar.");
            return;
        }
        String sql = "INSERT OR REPLACE INTO RedditArticleData (SubredditName, Author, LinkToSite, Sentiment, Date) VALUES (?, ?, ?, ?, ?)";
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + DB_PATH);
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            int savedRecords = 0;
            for (Map<String, Object> data : dataList) {
                stmt.setString(1, (String) data.get("subreddit_name"));
                stmt.setString(2, (String) data.get("author"));
                stmt.setString(3, (String) data.get("link_to_site"));
                stmt.setNull(4, java.sql.Types.VARCHAR);
                stmt.setTimestamp(5, (Timestamp) data.get("date"));

                savedRecords += stmt.executeUpdate();
            }
            logger.info("Se guardaron/actualizaron {} registros de Reddit en la base de datos.", savedRecords);
        }
    }

    private void setDoubleOrNull(PreparedStatement stmt, int index, Object value) throws SQLException {
        if (value == null) {
            stmt.setNull(index, java.sql.Types.REAL);
        } else {
            stmt.setDouble(index, (Double) value);
        }
    }

    private Timestamp parseBinanceDate(String dateStr) throws Exception {
        try {
            long epochMillis = Long.parseLong(dateStr);
            return new Timestamp(epochMillis);
        } catch (NumberFormatException e) {
            return new Timestamp(BINANCE_DATE_FORMAT.parse(dateStr).getTime());
        }
    }
}
