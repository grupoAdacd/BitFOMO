package APIs;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.net.http.HttpRequest;

import org.json.JSONArray;
import org.json.JSONObject;

public class BinanceAPI {
    private static final String BASE_URL = "https://api.binance.com";

    public JSONObject fetchTodayFluctuation(String correlation) throws IOException, InterruptedException {
        String apiURL = BASE_URL + "/api/v3/ticker/24hr?symbol=" + correlation;
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(apiURL))
                .build();
        HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
        String bodyResponse = response.body();

        return new JSONObject(bodyResponse);

    }

    public JSONArray fetchWhenFluctuation(String correlation, long startTime, long endTime, String period) throws IOException, InterruptedException {
        String apiURL = String.format("%s/api/v3/klines?symbol=%s&interval=%s&startTime=%d&endTime=%d&limit=1000", BASE_URL, correlation, period, startTime, endTime);
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(apiURL))
                .build();
        HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
        String bodyResponse = response.body();

        if (isValid(bodyResponse)) {
            return new JSONArray(bodyResponse);
        } else {
            System.err.println("Invalid response, raw response: " + bodyResponse);
            return null;
        }
    }

    public boolean isValid(String bodyResponse) {
        return bodyResponse.startsWith("[");
    }


}
