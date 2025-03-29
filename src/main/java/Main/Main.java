package Main;
import APIUtils.RedditAPIUtils;
import APIUtils.BinanceAPIUtils;


public class Main {
    public static void main(String[] args) {
        try{
            System.out.println("Starting App...");
            System.out.println("Data Collecting has started...");
            RedditAPIUtils.main(args);
            BinanceAPIUtils.main(args);
        } catch (Exception e) {
            System.err.println("Data Collecting Aborted..."+ e);
        }
    }
}