package cassdemo;

import java.io.IOException;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;
import cassdemo.backend.SetupSession;

public class Main {

    private static final String PROPERTIES_FILENAME = "config.properties";
    public static void main(String[] args) throws IOException, BackendException {
        String contactPoint = null;
        String keyspace = null;
        Properties properties = new Properties();
        try {
            properties.load(Main.class.getClassLoader().getResourceAsStream(PROPERTIES_FILENAME));

            contactPoint = properties.getProperty("contact_point");
            keyspace = properties.getProperty("keyspace");
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        // -----------------------------------------------------------------------------
		// przed prawidłowym skryptem na początku uruchomić tylko meotdy z setupSession,
        // potem zakomentowac
        // SetupSession setupSession = new SetupSession(contactPoint, keyspace);
        // setupSession.prepareStatements();
        // setupSession.setupCandidatesAndCitizens();
        // -----------------------------------------------------------------------------

        BackendSession session = new BackendSession(contactPoint, keyspace);
        session.voting();
		
        System.exit(0);
    }

}