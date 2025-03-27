package ProcessExtractedData;
import org.json.JSONArray;
import org.json.JSONObject;
import APIs.redditAPI;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.io.File;

public class redditAPIFetchUtils {
    private static final String REDDIT_DIRECTORY = "/home/d4rk/IdeaProjects/practiceAPIreddit/src/main/DataLake/DataLake/reddit_data";

    public static void main(String[] args) {
        int i;
        for (i=0;i<redditAPI.SUBREDDIT_CATEGORIES.size();i++) {
            extractUserData(redditAPI.fetchSubredditDataSinceTimestamp(redditAPI.SUBREDDIT_CATEGORIES.get(i), 1672531200));
        }
    }

    public static void extractUserData(JSONArray jsonArray) {
        int i;
        for (i=0; i< jsonArray.length();i++) {
            JSONObject userData = jsonArray.getJSONObject(i).getJSONObject("data");
            String author = userData.getString("author");
            String title = userData.getString("title");
            long dateInSeconds = userData.getLong("created");
            String text = userData.getString("selftext");
            String attachedNew = userData.getString("url");
            String linkToPost = userData.getString("permalink");
            int numberOfComments = userData.getInt("num_comments");
            long subredditSubscribers = userData.getLong("subreddit_subscribers");
            String subredditName = userData.getString("subreddit");

            insertOnFile(author, title, dateInSeconds, text, attachedNew, linkToPost, numberOfComments, subredditSubscribers, subredditName);
        }
    }
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
