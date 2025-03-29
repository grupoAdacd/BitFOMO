package Fetchers;
import org.json.JSONArray;
import APIs.BinanceAPI;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static InsertOnFIleUtils.BinanceInsertOnFileData.insertOnFileBinanceData;

public class BinanceDataFetcher {
    private static final String SYMBOL = "BTCUSDT";
    private static final String INTERVAL = "1h";
    public static JSONArray fetchBinanceData(BinanceAPI binanceAPI, LocalDateTime startDateTime,
                                             LocalDateTime endDateTime) {
        long startTimeMillis = startDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
        long endTimeMillis = endDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
        try {
            return binanceAPI.fetchWhenFluctuation(SYMBOL, startTimeMillis, endTimeMillis, INTERVAL);
        } catch (Exception apiError) {
            System.err.println("Error with Binance API call: " + apiError.getMessage());
            System.err.println("Make sure your BinanceAPI class is handling errors correctly.");
            System.err.println("Check if your API key has access to historical data.");
            return null;
        }
    }
    public static void fetchAndSaveData(BinanceAPI binanceAPI, LocalDateTime startDateTime,
                                        LocalDateTime endDateTime, String fileName) {
        try {
            JSONArray timeRangeData = fetchBinanceData(binanceAPI, startDateTime, endDateTime);
            insertOnFileBinanceData(timeRangeData, startDateTime, endDateTime, fileName);
        } catch (Exception e) {
            System.err.println("Error fetching or saving data for file " + fileName + ": " + e.getMessage());
            e.printStackTrace();
        }
    }
}
