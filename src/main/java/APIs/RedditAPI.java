package APIs;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONException;
import java.time.*;

public class RedditAPI {
    private static final String BASE_URL = "https://www.reddit.com";
    private static final String USER_AGENT = "BitFOMO/1.0 (by u/Interesting-Draw-517)";
    private static final int MAX_PAGES = 5;
    public static final ArrayList<String> SUBREDDIT_CATEGORIES = new ArrayList<>();

    static {
        SUBREDDIT_CATEGORIES.add("CryptoCurrency");
        SUBREDDIT_CATEGORIES.add("BitcoinNews");
        SUBREDDIT_CATEGORIES.add("Bitcoin");
        SUBREDDIT_CATEGORIES.add("btc");
    }

    public static JSONArray fetchSubredditDataSinceTimestamp(String subreddit, long sinceTimestamp) {
        JSONArray combinedPosts = new JSONArray();
        String after = null;
        boolean continueFetching = true;
        int pageCount = 0;
        System.out.println("Starting to fetch posts from r/" + subreddit + " since " +
                LocalDateTime.ofInstant(Instant.ofEpochSecond(sinceTimestamp), ZoneId.systemDefault()));
        while (continueFetching && pageCount < MAX_PAGES) {
            String apiURL = BASE_URL + "/r/" + subreddit + "/new.json?limit=100" +
                    (after != null ? "&after=" + after : "") + "?t=all";
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(apiURL))
                    .header("User-Agent", USER_AGENT)
                    .build();
            try {
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                String responseBody = response.body();
                System.out.println("Response Status Code: " + response.statusCode());
                System.out.println("Response Body (first 500 chars): " +
                        (responseBody != null ? responseBody.substring(0, Math.min(responseBody.length(), 500)) : "null"));
                if (responseBody == null || responseBody.trim().isEmpty()) {
                    System.err.println("Empty response body");
                    break;
                }
                if (!responseBody.trim().startsWith("{")) {
                    System.err.println("Invalid JSON response");
                    System.err.println("Full response body: " + responseBody);
                    break;
                }
                JSONObject responseBodyJSONObject;
                try {
                    responseBodyJSONObject = new JSONObject(responseBody);
                } catch (JSONException e) {
                    System.err.println("JSON Parsing Error: " + e.getMessage());
                    System.err.println("Problematic Response Body: " + responseBody);
                    break;
                }
                if (responseBodyJSONObject.has("error")) {
                    System.err.println("Reddit API Error: " + responseBodyJSONObject.getInt("error"));
                    break;
                }

                if (!responseBodyJSONObject.has("data") ||
                        !responseBodyJSONObject.getJSONObject("data").has("children")) {
                    System.out.println("No posts found in this page");
                    break;
                }

                JSONArray currentPagePosts = responseBodyJSONObject.getJSONObject("data").getJSONArray("children");

                boolean postsRemainingSinceCutoff = false;
                int postsAddedThisPage = 0;

                for (int i = 0; i < currentPagePosts.length(); i++) {
                    JSONObject post = currentPagePosts.getJSONObject(i).getJSONObject("data");
                    long postTimestamp = post.getLong("created");

                    if (postTimestamp >= sinceTimestamp) {
                        combinedPosts.put(currentPagePosts.getJSONObject(i));
                        postsAddedThisPage++;
                        postsRemainingSinceCutoff = true;
                    } else {
                        continueFetching = false;
                        break;
                    }
                }
                System.out.printf("Page %d: Added %d posts (Total: %d)%n",
                        pageCount + 1, postsAddedThisPage, combinedPosts.length());
                if (responseBodyJSONObject.getJSONObject("data").has("after")) {
                    after = responseBodyJSONObject.getJSONObject("data").getString("after");
                    pageCount++;
                    if (after == null || after.isEmpty() || !postsRemainingSinceCutoff) {
                        continueFetching = false;
                    }
                } else {
                    continueFetching = false;
                }

                Thread.sleep(1000);

            } catch(IOException | InterruptedException | JSONException e) {
                System.err.println("Error fetching Reddit data: " + e.getMessage());
                e.printStackTrace();
                break;
            }
        }

        System.out.println("Finished fetching. Total posts: " + combinedPosts.length());
        return combinedPosts;
    }
}
