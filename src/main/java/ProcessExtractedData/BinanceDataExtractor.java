package DataExtract;

import APIs.BinanceAPI;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Timer;
import java.util.TimerTask;

public class BinanceDataExtractor {
    private static final String SYMBOL = "BTCUSDT";
    private static final String INTERVAL = "1h";
    private static final int MONTHS_PER_FILE = 4;
    private static final String DATA_DIRECTORY = "/home/d4rk/IdeaProjects/BTCPriceEstimation/src/main/DataLake/DataLake/bitcoin_data";
    private static final DateTimeFormatter FILE_NAME_FORMAT = DateTimeFormatter.ofPattern("MMdyyyy");
    private static final DateTimeFormatter DISPLAY_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

    private static void fetchAndSaveData(BinanceAPI binanceAPI, LocalDateTime startDateTime,
                                         LocalDateTime endDateTime, String fileName) {
        try {
            long startTimeMillis = startDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
            long endTimeMillis = endDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
            try {
                JSONArray timeRangeData = binanceAPI.fetchWhenFluctuation(SYMBOL, startTimeMillis, endTimeMillis, INTERVAL);
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
                    }
                } else {
                    System.out.println("No data available for period: " +
                            startDateTime.format(DISPLAY_FORMAT) + " to " +
                            endDateTime.format(DISPLAY_FORMAT));
                }
            } catch (Exception apiError) {
                System.err.println("Error with Binance API call: " + apiError.getMessage());
                System.err.println("Make sure your BinanceAPI class is handling errors correctly.");
                System.err.println("Check if your API key has access to historical data.");
            }
        } catch (Exception e) {
            System.err.println("Error fetching or saving data for file " + fileName + ": " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void setupRealTimeCollection(BinanceAPI binanceAPI) {
        Timer timer = new Timer();
        TimerTask dataCollectionTask = new TimerTask() {
            @Override
            public void run() {
                try {
                    LocalDateTime now = LocalDateTime.now();
                    int startMonth = ((now.getMonthValue() - 1) / MONTHS_PER_FILE) * MONTHS_PER_FILE + 1;
                    LocalDateTime quadrimesterStart = now.withDayOfMonth(1).withMonth(startMonth)
                            .withHour(0).withMinute(0).withSecond(0).withNano(0);

                    LocalDateTime quadrimesterEnd = quadrimesterStart.plusMonths(MONTHS_PER_FILE);

                    String currentQuadFileName = quadrimesterStart.format(FILE_NAME_FORMAT) +
                            quadrimesterEnd.format(FILE_NAME_FORMAT) + ".txt";
                    JSONObject currentData = binanceAPI.fetchTodayFluctuation(SYMBOL);
                    if (currentData != null) {
                        File dataFile = new File(DATA_DIRECTORY + File.separator + currentQuadFileName);
                        boolean fileExists = dataFile.exists();

                        try (FileWriter writer = new FileWriter(dataFile, true)) {
                            if (!fileExists) {
                                writer.write("Bitcoin Real-time Data (" + SYMBOL + ") - Quadrimester starting " +
                                        quadrimesterStart.format(DISPLAY_FORMAT) + "\n");
                                writer.write("Interval: " + INTERVAL + " (real-time updates)\n\n");
                            }
                            writer.write("Time (UTC): " + now.format(DISPLAY_FORMAT) + "\n");
                            writer.write("Last Price: " + currentData.getString("lastPrice") + "\n");
                            writer.write("High Price (24h): " + currentData.getString("highPrice") + "\n");
                            writer.write("Low Price (24h): " + currentData.getString("lowPrice") + "\n");
                            writer.write("Volume: " + currentData.getString("volume") + "\n");
                            writer.write("Price Change %: " + currentData.getString("priceChangePercent") + "%\n");
                            writer.write("------------------------------------\n");
                            System.out.println("Real-time data collected at " + now.format(DISPLAY_FORMAT) +
                                    " and saved to " + currentQuadFileName);
                        }
                    } else {
                        System.out.println("Failed to fetch real-time data at " + now.format(DISPLAY_FORMAT));
                    }
                } catch (Exception e) {
                    System.err.println("Error in real-time data collection: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        };
        long fourHoursInMillis = 4 * 60 * 60 * 1000;
        timer.scheduleAtFixedRate(dataCollectionTask, 0, fourHoursInMillis);
        System.out.println("Real-time data collection started. Data will be collected every 4 hours.");
    }
}
