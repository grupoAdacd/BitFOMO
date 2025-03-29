package DataProcessing;

import APIs.BinanceAPI;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import static Fetchers.BinanceDataFetcher.fetchAndSaveData;


public class BinanceHistoricalDataProcessor {
    private static final DateTimeFormatter FILE_NAME_FORMAT = DateTimeFormatter.ofPattern("MMdyyyy");
    private static final DateTimeFormatter DISPLAY_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
    private static final int MONTHS_PER_FILE = 4;

    public static void processHistoricalData(BinanceAPI binanceAPI, LocalDateTime startDate, LocalDateTime endDate) {
        try {
            LocalDateTime currentPeriodStart = startDate;
            while (currentPeriodStart.isBefore(endDate)) {
                LocalDateTime currentPeriodEnd = currentPeriodStart.plusMonths(MONTHS_PER_FILE);
                if (currentPeriodEnd.isAfter(endDate)) {
                    currentPeriodEnd = endDate;
                }
                String fileName = currentPeriodStart.format(FILE_NAME_FORMAT) +
                        currentPeriodEnd.format(FILE_NAME_FORMAT) + ".txt";
                System.out.println("Processing data for period: " +
                        currentPeriodStart.format(DISPLAY_FORMAT) + " to " +
                        currentPeriodEnd.format(DISPLAY_FORMAT));
                fetchAndSaveData(binanceAPI, currentPeriodStart, currentPeriodEnd, fileName);
                currentPeriodStart = currentPeriodEnd;
            }
            System.out.println("Historical data collection completed.");
        } catch (Exception e) {
            System.err.println("Error processing historical data: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
