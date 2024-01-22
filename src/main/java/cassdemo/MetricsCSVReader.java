package cassdemo;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MetricsCSVReader {
    private static final String METRICS_FILES_PATH = "";
    private static final Map<String, String> METRIC_FILES;
    static {
        METRIC_FILES = Stream.of(
            new String[]{"client-timeouts.csv", "Client timeouts"},
            new String[]{"connected-to.csv", "Connected to"},
            new String[]{"connection-errors.csv", "Connection Errors"},
            new String[]{"open-connections.csv", "Open Connections"},
            new String[]{"read-timeouts.csv", "Read Timeouts"},
            new String[]{"retries-on-client-timeout.csv", "Retries on Client Timeout"},
            new String[]{"retries-on-connection-error.csv", "Retries on Connection Error"},
            new String[]{"retries-on-other-errors.csv", "Retries on Other Errors"},
            new String[]{"retries-on-read-timeout.csv", "Retries on Read Timeout"},
            new String[]{"retries-on-unavailable.csv", "Retries on Unavailable"},
            new String[]{"retries-on-write-timeout.csv", "Retries on Write Timeout"},
            new String[]{"retries.csv", "Total Retries"},
            new String[]{"unavailables.csv", "Unavailable"},
            new String[]{"write-timeouts.csv", "Retries on Write Timeouts"}
        ).collect(Collectors.toMap(data -> data[0], data -> data[1]));
    };
    
    private static final String REQUEST_FILE_NAME = "requests.csv";

    public static void main(String[] args) throws IOException, CsvException, InterruptedException {

        while (true) {
            for (Map.Entry<String, String> entry : METRIC_FILES.entrySet()) {
                String filename = entry.getKey();
                String label = entry.getValue();

                try (CSVReader reader = new CSVReader(new FileReader(METRICS_FILES_PATH + filename))) {

                    // // Read the last row
                    // String[] lastRow = null;
                    // String[] currentRow;
                    // while ((currentRow = reader.readNext()) != null) {
                    //     lastRow = currentRow;
                    // }

                    List<String[]> rows = reader.readAll();
                    Collections.reverse(rows);

                    // Display information
                    if (!rows.isEmpty()) {
                        String[] lastRow = rows.get(0);

                        String timestamp = lastRow[0]; // Assuming timestamp is at index 0
                        String value = lastRow[1];     // Assuming value is at index 1
                        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(Long.valueOf(timestamp)), ZoneId.systemDefault());
                        System.out.println("[" + dateTime + "] " + label + ": " + value);
                    } else {
                        System.out.println("No data in file: " + filename);
                    }
                } catch (IOException | CsvException e) {
                    e.printStackTrace();
                }
            }
            handleRequestsFile();
            Thread.sleep(10000);
        }
        
    }

    private static void handleRequestsFile() {
        try (CSVReader reader = new CSVReader(new FileReader(METRICS_FILES_PATH + REQUEST_FILE_NAME))) {
            // Read all rows from the CSV file
            List<String[]> rows = reader.readAll();

            // Reverse the list to process the newest line first
            Collections.reverse(rows);

            // Process each row, but only the newest line
            if (!rows.isEmpty()) {
                String[] fields = rows.get(0);
                // Extract relevant fields
                long timestamp = Long.parseLong(fields[0]);
                long count = Long.parseLong(fields[1]);
                double max = Double.parseDouble(fields[2]);
                double mean = Double.parseDouble(fields[3]);
                double min = Double.parseDouble(fields[4]);
                double m1Rate = Double.parseDouble(fields[13]);
                double m5Rate = Double.parseDouble(fields[14]);

                // Convert timestamp to LocalDateTime
                LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(timestamp), ZoneId.systemDefault());

                // Print the information
                System.out.printf("[%s] Count: %d, Max: %.2f/s, Mean: %.2f/s, Min: %.2f/s, Last minute: %.2f/s, Last 5 minutes: %.2f/s \n",
                        dateTime, count, max, mean, min, m1Rate, m5Rate);
                System.out.println();
            }
        } catch (IOException | CsvException e) {
            e.printStackTrace();
        }
    }
}
