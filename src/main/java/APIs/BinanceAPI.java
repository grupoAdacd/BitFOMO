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

}
