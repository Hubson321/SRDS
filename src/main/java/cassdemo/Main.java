package cassdemo;

import java.io.IOException;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import cassdemo.backend.BackendException;
import cassdemo.backend.BackendSession;
import cassdemo.backend.SetupSession;
import cassdemo.backend.AreaThread;
public class Main {

    private static final String PROPERTIES_FILENAME = "config.properties";
    public static void main(String[] args) throws IOException, BackendException, InterruptedException {
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
        // robimy liczniki w customretry
        // dodać metodę do printowania w customretrypolicy
        // funckje do printowania wywołamy w main

        // -----------------------------------------------------------------------------
		// przed prawidłowym skryptem na początku uruchomić tylko meotdy z setupSession,
        // potem zakomentowac
        // SetupSession setupSession = new SetupSession(contactPoint, keyspace);
        // setupSession.prepareStatements();
        // setupSession.setupCandidatesAndCitizens();
        // -----------------------------------------------------------------------------

        String[] contactPoints = {"172.18.0.1","172.18.0.2","172.18.0.3"};
        BackendSession session = new BackendSession(contactPoints, keyspace);
       
        
		int numAreas = 10;
		AreaThread[] areas = new AreaThread[numAreas];
		DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");

        LocalDateTime maxDateTime = LocalDateTime.parse("18-01-2024 21:57:00", inputFormatter); // do manipulacji daty konca glosowania (dzien-miesiac-rok godzina-minuta-sekunda)
        System.out.println("----------------------------------------------------------");
        System.out.println("Poczatek glosowania");
        System.out.println("----------------------------------------------------------");
		for (int i = 0; i < numAreas; i++) {
			areas[i] = new AreaThread(session, maxDateTime);
			areas[i].t.start();
		}

		try {
			for (int i = 0; i < numAreas; i++) {
				areas[i].t.join();
			}
		} catch (InterruptedException e) {
			System.out.println("Thread Interrupted");
		}

        session.saveFrequency();
        System.out.println("----------------------------------------------------------");
		System.out.println("Koniec glosowania");
        System.out.println("----------------------------------------------------------");
        // session.displayFinalResults();
        session.displayFrequency();
        session.printErrors();
        System.exit(0);
    }

}