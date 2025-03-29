package InsertOnFIleUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class RedditInsertOnFileData {
    private static final String REDDIT_DIRECTORY = "/home/d4rk/IdeaProjects/practiceAPIreddit/src/main/DataLake/DataLake/reddit_data";
    public static void insertOnFile(String author, String title, long dateInSeconds,
                                    String text, String attachedNew, String linkToPost,
                                    int numberOfComments, long subredditSubscribers, String subredditName) {
        try {
            long milliseconds = dateInSeconds * 1000;
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            String formattedDate = dateFormat.format(new Date(milliseconds));
            String filename = formattedDate + String.format("_%s_data.txt", subredditName);

            File outputDirectory = new File(REDDIT_DIRECTORY);
            if (!outputDirectory.exists()) {
                outputDirectory.mkdirs();
            }
            File fullpath = new File(outputDirectory, formattedDate + String.format("_%s_data.txt", subredditName));
            if (!fullpath.exists()) {
                fullpath.createNewFile();
            }
            try (FileWriter writer = new FileWriter(fullpath, true);
                 BufferedWriter bufferedWriter = new BufferedWriter(writer)) {

                bufferedWriter.write("Author: " + author);
                bufferedWriter.newLine();
                bufferedWriter.write("Title: " + title);
                bufferedWriter.newLine();
                bufferedWriter.write("Date: " + new Date(milliseconds));
                bufferedWriter.newLine();
                bufferedWriter.write("Text: " + text);
                bufferedWriter.newLine();
                bufferedWriter.write("URL: " + attachedNew);
                bufferedWriter.newLine();
                bufferedWriter.write("Permalink: " + linkToPost);
                bufferedWriter.newLine();
                bufferedWriter.write("Number of Comments: " + numberOfComments);
                bufferedWriter.newLine();
                bufferedWriter.write("Subreddit Subscribers: " + subredditSubscribers);
                bufferedWriter.newLine();
                bufferedWriter.write("------------------------------------------------------------");
                bufferedWriter.newLine();
                System.out.println(String.format("Writing into %s...", filename));
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Error writing to file: " + e.getMessage());
        } catch (NumberFormatException e) {
            e.printStackTrace();
            System.err.println("Invalid date format: " + e.getMessage());
        }
    }
}
