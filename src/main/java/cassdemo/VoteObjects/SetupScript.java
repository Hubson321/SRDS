import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import java.time.Instant;
import java.util.UUID;

public class CassandraInsertScript {


        private final BackendSess

            for (int i = 0; i <= 28000000; i++) {
                UUID userId = UUID.randomUUID();
                int area = i % 10;
                Instant eventTime = Instant.now();

                try {
                    insertUser(session, area, userId, eventTime);
                } catch (Exception e) {
                    System.err.println("Error inserting user: " + e.getMessage());
                }
            }

    }

    private static void createKeyspaceAndTable(CqlSession session) {
        session.execute("CREATE KEYSPACE IF NOT EXISTS your_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        session.execute("USE your_keyspace");

        session.execute("CREATE TABLE IF NOT EXISTS person_data (" +
                "area INT," +
                "idPerson UUID," +
                "eventTime TIMESTAMP," +
                "PRIMARY KEY (area, idPerson, eventTime))");
    }

    private static void insertUser(CqlSession session, int area, UUID userId, Instant eventTime) {
        String insertQuery = "INSERT INTO person_data (area, idPerson, eventTime) VALUES (?, ?, ?)";
        SimpleStatement statement = SimpleStatement.builder(insertQuery)
                .addPositionalValues(area, userId, eventTime)
                .build();

        try {
            session.execute(statement);
        } catch (AllNodesFailedException e) {
            // Handle exception specific to connection issues, like all nodes being down
            throw new RuntimeException("Failed to execute statement: " + e.getMessage(), e);
        } catch (Exception e) {
            // Handle other exceptions
            throw new RuntimeException("Error executing statement: " + e.getMessage(), e);
        }
    }
}
