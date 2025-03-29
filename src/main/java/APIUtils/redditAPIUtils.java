package APIUtils;
import APIs.redditAPI;

import static Fetchers.RedditDataExtractor.extractUserData;

public class redditAPIUtils {

    public static void main(String[] args) {
        int i;
        for (i=0;i<redditAPI.SUBREDDIT_CATEGORIES.size();i++) {
            extractUserData(redditAPI.fetchSubredditDataSinceTimestamp(redditAPI.SUBREDDIT_CATEGORIES.get(i), 0));
        }
    }

}
