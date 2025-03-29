package InsertOnFIleUtils;
import org.json.JSONArray;

import java.io.IOException;
import java.time.LocalDateTime;
import java.io.File;
import java.io.FileWriter;
import java.time.format.DateTimeFormatter;
import java.time.ZoneOffset;

public class binanceInsertOnFileData {
    private static final String SYMBOL = "BTCUSDT";
    private static final String INTERVAL = "1h";
    private static final String DATA_DIRECTORY = "/home/d4rk/IdeaProjects/BTCPriceEstimation/src/main/DataLake/DataLake/bitcoin_data";
    private static final DateTimeFormatter DISPLAY_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

    public static void insertOnFileBinanceData(JSONArray timeRangeData, LocalDateTime startDateTime,
                                               LocalDateTime endDateTime, String fileName) throws IOException {
        if (timeRangeData != null && timeRangeData.length() > 0) {
            File dataFile = new File(DATA_DIRECTORY + File.separator + fileName);
            try (FileWriter writer = new FileWriter(dataFile)) {
                writer.write("Bitcoin Price Data (" + SYMBOL + ") from " +
                        startDateTime.format(DISPLAY_FORMAT) + " to " +
                        endDateTime.format(DISPLAY_FORMAT) + "\n");
                writer.write("Interval: " + INTERVAL + "\n\n");
                for (int i = 0; i < timeRangeData.length(); i++) {
                    JSONArray candle = timeRangeData.getJSONArray(i);
                    LocalDateTime candleTime = LocalDateTime.ofEpochSecond(
                            candle.getLong(0) / 1000, 0, ZoneOffset.UTC);
                    writer.write("Time (UTC): " + candleTime.format(DISPLAY_FORMAT) + "\n");
                    writer.write("Open Price: " + candle.getString(1) + "\n");
                    writer.write("High Price: " + candle.getString(2) + "\n");
                    writer.write("Low Price: " + candle.getString(3) + "\n");
                    writer.write("Close Price: " + candle.getString(4) + "\n");
                    writer.write("Volume: " + candle.getString(5) + "\n");
                    writer.write("------------------------------------\n");
                }
                System.out.println("Data saved to file: " + fileName +
                        " (" + timeRangeData.length() + " data points)");
            } catch (IOException e) {
                System.err.println("Error writing to file " + fileName + ": " + e.getMessage());
                e.printStackTrace();
            }
        } else {
            System.out.println("No data available for period: " +
                    startDateTime.format(DISPLAY_FORMAT) + " to " +
                    endDateTime.format(DISPLAY_FORMAT));
        }
    }
}
