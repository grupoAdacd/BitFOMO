package APIUtils;
import APIs.RedditAPI;

import static Fetchers.RedditDataFetcher.extractUserData;

public class RedditAPIUtils {

    public static void main(String[] args) {
        int i;
        for (i=0; i< RedditAPI.SUBREDDIT_CATEGORIES.size(); i++) {
            extractUserData(RedditAPI.fetchSubredditDataSinceTimestamp(RedditAPI.SUBREDDIT_CATEGORIES.get(i), 0));
        }
    }

}
