# 🚀 BitFOMO: Predicting Crypto Trends with Reddit & Binance 📈

## Unlocking Market Insights Through Sentiment and Data

---

![Reddit Icon](https://img.icons8.com/color/48/000000/reddit.png) + 💰 (Binance Data) = 🔮 (Predictions & Metrics)
---

A project that analyzes Reddit posts and Binance financial information to provide cryptocurrency predictions and key metrics.

---

### Module Descriptions

* **`DataLake`**: This module is responsible for the initial storage of raw data fetched from external sources.
    * **`DataLake`**: Likely contains classes for managing the storage of unprocessed data.
    * **`DataMart`**: This module likely handles the storage of processed and aggregated data that is ready for analysis and prediction.
* **`java`**: This module contains the core Java source code for the application.
    * **`APIs`**: This package includes classes for interacting with external APIs.
        * **`BinanceAPI.java`**: Handles communication with the Binance API to fetch financial data (e.g., price, volume).
        * **`redditAPI.java`**: Manages interactions with the Reddit API to retrieve post data from relevant subreddits.
    * **`Main`**: This package contains the entry point of the application.
        * **`Main.java`**: The main class that orchestrates the data fetching, processing, and prediction logic.
    * **`ProcessExtractedData`**: This package includes classes responsible for processing the raw data extracted from the APIs.
        * **`BinanceDataExtractor.java`**: Contains logic to extract and process relevant financial information from the data fetched from Binance.
        * **`redditAPIFetchUtils.java`**: Provides utility functions to fetch and potentially parse data from Reddit posts.
        * **`ProcessExtractedData.java`**: Might contain a class to coordinate the processing of data from both sources.
* **`resources`**: This directory likely contains configuration files or other static resources needed by the application.
* **`.gitignore`**: Specifies intentionally untracked files that Git should ignore.
* **`pom.xml`**: The Project Object Model (POM) file for Maven, defining project dependencies and build configurations.
* **`test`**: Contains unit and integration tests for the project.
* **`External Libraries`**: Lists the external libraries and dependencies used by the project (managed by Maven).
* **`Scratches and Consoles`**: IntelliJ IDEA specific directory for temporary files and consoles.

## Functionality

The BitFOMO project aims to provide the following functionalities:

1.  **Data Acquisition**:
    * Fetch real-time or historical financial data for cryptocurrencies (likely Bitcoin and related assets) from the Binance API.
    * Retrieve posts and comments from relevant cryptocurrency subreddits on Reddit using the Reddit API.
2.  **Data Processing and Analysis**:
    * Process the financial data to extract relevant metrics (e.g., price trends, volume changes).
    * Analyze the text content of Reddit posts to gauge sentiment and identify trending topics related to cryptocurrencies.
    * Potentially correlate social media sentiment with financial market movements.
3.  **Prediction and Metrics**:
    * Generate predictions on the future price movements or trends of cryptocurrencies based on the analyzed data.
    * Provide users with insightful metrics derived from the combined analysis of Reddit and Binance data. These metrics could include sentiment scores, trending topics, and potential market signals.

## User Interface (Conceptual)

While the project structure doesn't explicitly define a user interface, the goal is to provide predictions and metrics to users. This could be achieved through various means:

* **Command-line interface (CLI)**: Users could run the application from the command line to get the latest predictions and metrics.
* **Web application**: A web interface could be developed to display the information in a user-friendly manner.
* **API endpoint**: The project could expose an API endpoint that other applications can consume to access the predictions and metrics.

## Potential Use Cases

* **Informed Trading Decisions**: Users can use the insights provided by BitFOMO to make more informed decisions when trading cryptocurrencies.
* **Market Sentiment Analysis**: The platform can help users understand the overall market sentiment towards specific cryptocurrencies based on Reddit discussions.
* **Trend Identification**: BitFOMO can assist in identifying emerging trends and topics within the cryptocurrency community that might impact market prices.

## Conclusion

BitFOMO is a promising project that combines social media analysis with financial data to provide valuable insights into the cryptocurrency market. By leveraging the power of community sentiment and market dynamics, it aims to empower users with data-driven predictions and metrics.
