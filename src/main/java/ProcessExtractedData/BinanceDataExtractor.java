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
