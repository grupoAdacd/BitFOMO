package Main;

import APIUtils.RedditAPIUtils;
import APIUtils.BinanceAPIUtils;
import org.example.datalake.DataLakeToDatabaseMigrator;

public class Main {
    public static void main(String[] args) {
        try {
            System.out.println("Starting App...");

            System.out.println("Migrating existing DataLake data to database...");
            DataLakeToDatabaseMigrator migrator = new DataLakeToDatabaseMigrator();
            migrator.migrateDataLakeToDatabase();

            System.out.println("Data Collecting has started...");
            RedditAPIUtils.main(args);
            BinanceAPIUtils.main(args);

            System.out.println("Migrating new DataLake data to database...");
            migrator.migrateDataLakeToDatabase();

            System.out.println("App execution completed successfully.");
        } catch (Exception e) {
            System.err.println("Data Collecting Aborted: " + e.getMessage());
            e.printStackTrace();
        }
    }
}