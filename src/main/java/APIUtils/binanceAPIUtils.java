package APIUtils;

import APIs.BinanceAPI;
import org.json.JSONArray;
import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static DataProcessing.BinanceHistoricalDataProcessor.processHistoricalData;
import static RealTimeCollectors.BinanceRealtimeCollector.setupRealTimeCollection;

public class binanceAPIUtils {
    private static final String INTERVAL = "1h";
    private static final String DATA_DIRECTORY = "/home/d4rk/IdeaProjects/BTCPriceEstimation/src/main/DataLake/DataLake/bitcoin_data";
    private static final DateTimeFormatter DISPLAY_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

    public static void main(String[] args) {
        try {
            File directory = new File(DATA_DIRECTORY);
            if (!directory.exists()) {
                directory.mkdirs();
            }
            BinanceAPI binanceAPI = new BinanceAPI();
            LocalDateTime startDate = LocalDateTime.of(2023, 1, 1, 0, 0);
            LocalDateTime currentDate = LocalDateTime.now();
            System.out.println("Starting Bitcoin data collection from " + startDate.format(DISPLAY_FORMAT) +
                    " to " + currentDate.format(DISPLAY_FORMAT));
            System.out.println("Using interval: " + INTERVAL + " (Binance supported interval closest to 5h)");
            processHistoricalData(binanceAPI, startDate, currentDate);
            setupRealTimeCollection(binanceAPI);
        } catch (Exception e) {
            System.err.println("Error in Bitcoin data collection: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
