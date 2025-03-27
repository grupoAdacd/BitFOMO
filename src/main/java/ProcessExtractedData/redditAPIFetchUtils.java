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
}
