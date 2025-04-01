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
import java.sql.Types;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Clase para migrar datos del DataLake (archivos .txt) a la base de datos SQLite (bitFOMO.db).
 * Se recrea el esquema para que la fecha se guarde como texto legible y se extraiga el nombre del subreddit desde el permalink.
 */
public class DataLakeToDatabaseMigrator {
    private static final Logger logger = LoggerFactory.getLogger(DataLakeToDatabaseMigrator.class);

    private static final String DB_PATH = "src/main/DataLake/DataMarkt/bitFOMO.db";
    private static final String BINANCE_DATA_DIR = "src/main/DataLake/DataLake/bitcoin_data";
    private static final String REDDIT_DATA_DIR = "src/main/DataLake/DataLake/reddit_data";

    // Para parsear fechas de Binance: pueden venir en "yyyy-MM-dd HH:mm" o como epoch (número)
    private static final SimpleDateFormat BINANCE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    // Para parsear fechas de Reddit (en inglés), usando Locale.US
    private static final SimpleDateFormat REDDIT_DATE_FORMAT = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);
    // Formato de salida para guardar la fecha como texto (por ejemplo, "2025-02-11 15:00:00")
    private static final SimpleDateFormat OUTPUT_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * Este método recrea el esquema de la base de datos (eliminando las tablas existentes)
     * y creando las tablas con la estructura completa, guardando las fechas como texto.
     */
    public void migrateSchema() {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + DB_PATH)) {
            // Eliminar tablas existentes
            conn.createStatement().execute("DROP TABLE IF EXISTS FinantialData");
            conn.createStatement().execute("DROP TABLE IF EXISTS RedditArticleData");

            // Crear tabla FinantialData; la columna Date es TEXT para almacenar la fecha formateada
            String sqlFinantial = "CREATE TABLE FinantialData (" +
                    "Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    "VolumeDay REAL, " +
                    "OpenPrice REAL, " +
                    "LowestPriceDay REAL, " +
                    "HighestPriceDay REAL, " +
                    "PriceChangePercent REAL, " +
                    "CurrentPrice REAL, " +
                    "Date TEXT" +
                    ")";
            conn.createStatement().execute(sqlFinantial);

            // Crear tabla RedditArticleData sin la columna Permalink
            String sqlReddit = "CREATE TABLE RedditArticleData (" +
                    "Id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    "SubredditName TEXT, " +
                    "Author TEXT, " +
                    "Title TEXT, " +
                    "Selftext TEXT, " +
                    "LinkToSite TEXT, " +
                    "NumComments INTEGER, " +
                    "SubredditSubscribers INTEGER, " +
                    "Sentiment TEXT, " +
                    "Date TEXT" +
                    ")";
            conn.createStatement().execute(sqlReddit);

            logger.info("Esquema migrado/recreado correctamente.");
        } catch (SQLException e) {
            logger.error("Error durante la migración del esquema: {}", e.getMessage(), e);
            throw new RuntimeException("Error en la migración del esquema", e);
        }
    }

    /**
     * Método principal que migra primero el esquema y luego los datos del DataLake a la base de datos.
     */
    public void migrateDataLakeToDatabase() {
        try {
            logger.info("Iniciando migración de datos del DataLake a la base de datos...");

            migrateSchema();

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

                    // Saltar encabezados innecesarios
                    if (line.startsWith("Bitcoin Price Data") ||
                            line.startsWith("Bitcoin Real-time Data") ||
                            line.startsWith("Interval:")) {
                        continue;
                    }

                    // Separador entre registros
                    if (line.startsWith("------------------------------------")) {
                        if (data != null && !data.isEmpty()) {
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
        // Guardar fecha como texto formateada
        String sql = "INSERT OR REPLACE INTO FinantialData " +
                "(VolumeDay, OpenPrice, LowestPriceDay, HighestPriceDay, PriceChangePercent, CurrentPrice, Date) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + DB_PATH);
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            int savedRecords = 0;
            int skippedRecords = 0;
            for (Map<String, Object> data : dataList) {
                if (data.get("volume_day") == null || data.get("open_price") == null ||
                        data.get("lowest_price_day") == null || data.get("highest_price_day") == null ||
                        data.get("current_price") == null || data.get("date") == null) {
                    logger.warn("Registro de Binance incompleto: {}", data);
                    skippedRecords++;
                    continue;
                }
                stmt.setDouble(1, (Double) data.get("volume_day"));
                stmt.setDouble(2, (Double) data.get("open_price"));
                stmt.setDouble(3, (Double) data.get("lowest_price_day"));
                stmt.setDouble(4, (Double) data.get("highest_price_day"));
                setDoubleOrNull(stmt, 5, data.get("price_change_percent"));
                stmt.setDouble(6, (Double) data.get("current_price"));
                String formattedDate = OUTPUT_DATE_FORMAT.format((Timestamp) data.get("date"));
                stmt.setString(7, formattedDate);
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
                // Inicializamos el Map para cada registro.
                Map<String, Object> data = new HashMap<>();
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;

                    // Si aparece una línea que indica "Reddit Data for", se intenta extraer el nombre del subreddit.
                    if (line.startsWith("Reddit Data for")) {
                        // Ejemplo: "Reddit Data for r/BitcoinNews (BitcoinNews)"
                        int start = line.indexOf("(");
                        int end = line.indexOf(")");
                        if (start >= 0 && end > start) {
                            String subreddit = line.substring(start + 1, end).trim();
                            data.put("subreddit_name", subreddit);
                        }
                        continue;
                    }

                    // Cuando aparece "Permalink:" se extrae el nombre del subreddit a partir de la URL
                    if (line.startsWith("Permalink:")) {
                        String permalink = line.substring("Permalink:".length()).trim();
                        int idx = permalink.indexOf("/r/");
                        if (idx != -1) {
                            int startSub = idx + 3; // después de "/r/"
                            int endSub = permalink.indexOf("/", startSub);
                            if (endSub != -1) {
                                String extractedSub = permalink.substring(startSub, endSub);
                                data.put("subreddit_name", extractedSub);
                            }
                        }
                        continue;
                    }

                    // Separador entre registros
                    if (line.startsWith("------------------------------------------------------------")) {
                        if (!data.isEmpty()) {
                            dataList.add(data);
                        }
                        data = new HashMap<>();
                        continue;
                    }

                    if (line.startsWith("Author:")) {
                        data.put("author", line.substring("Author:".length()).trim());
                    } else if (line.startsWith("Title:")) {
                        data.put("title", line.substring("Title:".length()).trim());
                    } else if (line.startsWith("Date:")) {
                        String dateStr = line.substring("Date:".length()).trim();
                        try {
                            data.put("date", new Timestamp(REDDIT_DATE_FORMAT.parse(dateStr).getTime()));
                        } catch (Exception e) {
                            logger.error("Error parseando fecha en archivo Reddit {}: {}", file.getName(), e.getMessage());
                            data.put("date", null);
                        }
                    } else if (line.startsWith("Text:")) {
                        data.put("selftext", line.substring("Text:".length()).trim());
                    } else if (line.startsWith("URL:")) {
                        data.put("link_to_site", line.substring("URL:".length()).trim());
                    } else if (line.startsWith("Number of Comments:")) {
                        try {
                            data.put("num_comments", Integer.parseInt(line.substring("Number of Comments:".length()).trim()));
                        } catch (NumberFormatException e) {
                            logger.error("Error parseando Number of Comments en archivo {}: {}", file.getName(), e.getMessage());
                        }
                    } else if (line.startsWith("Subreddit Subscribers:")) {
                        try {
                            data.put("subreddit_subscribers", Integer.parseInt(line.substring("Subreddit Subscribers:".length()).trim()));
                        } catch (NumberFormatException e) {
                            logger.error("Error parseando Subreddit Subscribers en archivo {}: {}", file.getName(), e.getMessage());
                        }
                    } else if (line.startsWith("Subreddit:")) {
                        data.put("subreddit_name", line.substring("Subreddit:".length()).trim());
                    }
                }
                if (!data.isEmpty()) {
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
        // Se elimina la columna Permalink del INSERT
        String sql = "INSERT OR REPLACE INTO RedditArticleData " +
                "(SubredditName, Author, Title, Selftext, LinkToSite, NumComments, SubredditSubscribers, Sentiment, Date) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + DB_PATH);
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            int savedRecords = 0;
            for (Map<String, Object> data : dataList) {
                stmt.setString(1, (String) data.get("subreddit_name"));
                stmt.setString(2, (String) data.get("author"));
                stmt.setString(3, (String) data.get("title"));
                stmt.setString(4, (String) data.get("selftext"));
                stmt.setString(5, (String) data.get("link_to_site"));
                if (data.get("num_comments") != null) {
                    stmt.setInt(6, (Integer) data.get("num_comments"));
                } else {
                    stmt.setNull(6, Types.INTEGER);
                }
                if (data.get("subreddit_subscribers") != null) {
                    stmt.setInt(7, (Integer) data.get("subreddit_subscribers"));
                } else {
                    stmt.setNull(7, Types.INTEGER);
                }
                // Sentiment no se extrae, se guarda NULL.
                stmt.setNull(8, Types.VARCHAR);
                if (data.get("date") != null) {
                    String formattedDate = OUTPUT_DATE_FORMAT.format((Timestamp) data.get("date"));
                    stmt.setString(9, formattedDate);
                } else {
                    stmt.setNull(9, Types.VARCHAR);
                }
                savedRecords += stmt.executeUpdate();
            }
            logger.info("Se guardaron/actualizaron {} registros de Reddit en la base de datos.", savedRecords);
        }
    }

    private void setDoubleOrNull(PreparedStatement stmt, int index, Object value) throws SQLException {
        if (value == null) {
            stmt.setNull(index, Types.REAL);
        } else {
            stmt.setDouble(index, (Double) value);
        }
    }

    /**
     * Método para parsear la fecha de Binance.
     * Intenta primero interpretar la cadena como número (epoch en milisegundos),
     * y si falla, utiliza el formato "yyyy-MM-dd HH:mm".
     *
     * @param dateStr La cadena de fecha.
     * @return Timestamp parseado.
     * @throws Exception Si falla el parseo.
     */
    private Timestamp parseBinanceDate(String dateStr) throws Exception {
        try {
            long epochMillis = Long.parseLong(dateStr);
            return new Timestamp(epochMillis);
        } catch (NumberFormatException e) {
            return new Timestamp(BINANCE_DATE_FORMAT.parse(dateStr).getTime());
        }
    }
}
