package Main;
import APIUtils.redditAPIUtils;
import APIUtils.binanceAPIUtils;


public class Main {
    public static void main(String[] args) {
        try{
            System.out.println("Starting App...");
            System.out.println("Data Collecting has started...");
            redditAPIUtils.main(args);
            binanceAPIUtils.main(args);
        } catch (Exception e) {
            System.err.println("Data Collecting Aborted..."+ e);
        }
    }
}