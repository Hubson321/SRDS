package cassdemo.backend;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/*
 * For error handling done right see: 
 * https://www.datastax.com/dev/blog/cassandra-error-handling-done-right
 * 
 * Performing stress tests often results in numerous WriteTimeoutExceptions, 
 * ReadTimeoutExceptions (thrown by Cassandra replicas) and 
 * OpetationTimedOutExceptions (thrown by the client). Remember to retry
 * failed operations until success (it can be done through the RetryPolicy mechanism:
 * https://stackoverflow.com/questions/30329956/cassandra-datastax-driver-retry-policy )
 */

public class BackendSession {

	private static final Logger logger = LoggerFactory.getLogger(BackendSession.class);

	private Session session;

	public BackendSession(String contactPoint, String keyspace) throws BackendException {

		Cluster cluster = Cluster.builder().addContactPoint(contactPoint).build();
		try {
			session = cluster.connect(keyspace);
		} catch (Exception e) {
			throw new BackendException("Could not connect to the cluster. " + e.getMessage() + ".", e);
		}
		prepareStatements();
	}

	private static PreparedStatement SELECT_ALL_FROM_USERS;
	private static PreparedStatement INSERT_INTO_USERS;
	private static PreparedStatement DELETE_ALL_FROM_USERS;
	private static PrepareStatement INSERT_CITIZEN;
	private static PrepareStatement INSERT_CANDIDATE;

	private static final String USER_FORMAT = "- %-10s  %-16s %-10s %-10s\n";
	// private static final SimpleDateFormat df = new
	// SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private void setupScript() throws BackendException {
		this.prepareCitizenSetup();
	}

	private void prepareCitizenSetup(){
		Integer areaNum = 1;
		for (int i = 0; i <= 28000000; i++) {
			UUID userId = UUID.randomUUID();
			// jeden z 41 okregów wyborczych
			// 28 mln / 41 ~ 682927 - tylu obywateli per okręg
			if( i % 682927 == 0){
				areaNum += 1;
			}

			try {
				insertCitizen(userId, areaNum);
			} catch (Exception e) {
				System.err.println("Error inserting user: " + e.getMessage());
			}
		}
	}

	private void prepareCandidateSetup(){
		Random random = new Random();
		Integer areaNum = 1;
		List<Candidate> personsList = Arrays.asList(objectMapper.readValue(new File("src/main/resource/candidates" +
				".json"),	Candidate[].class));
		Integer counter = 0;
		for (Person person : personsArray) {
			// umieszczanie kolejnych kandydatów w następnym okręgu wyborczym. Kazdy okręg po 28 kandydatów
			if(counter % 27 == 0){
				areaNum += 1;
			}
			String name = person.getName();
			String surname = person.getSurname();
			UUID candidateId = UUID.randomUUID();

			try {
				insertCandidate(areaNum, candidateId, name, surname);
				counter++;
			} catch (Exception e) {
				System.err.println("Error inserting user: " + e.getMessage());
			}
		}
	}

	private void insertCitizen(UUID citizenId, Integer areaNum) throws  BackendException{
		BoundStatement bs = new BoundStatement(INSERT_CITIZEN);
		bs.bind(areaNum, citizenId, false, false);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
		}
	}

	private void insertCandidate(Integer areaNum, UUID candidateId, String name, String surname) throws  BackendException{
		BoundStatement bs = new BoundStatement(INSERT_CANDIDATE);
		bs.bind(areaNum, citizenId, name, surname);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
		}
	}



	private void prepareCandiatesSetup() {
		Random random = new Random();
		for (int i = 0; i <= 28000000; i++) {
			UUID userId = UUID.randomUUID();
			// jeden z 41 okregów wyborczych
			Integer areaNum = random.nextInt(41) + 1;

			try {
				insertUser(session, area, userId, eventTime);
			} catch (Exception e) {
				System.err.println("Error inserting user: " + e.getMessage());
			}
		}
	}

	private void prepareStatements() throws BackendException {
		try {
			SELECT_ALL_FROM_USERS = session.prepare("SELECT * FROM users;");
			INSERT_INTO_USERS = session
					.prepare("INSERT INTO users (companyName, name, phone, street) VALUES (?, ?, ?, ?);");
			INSERT_CITIZEN = session.prepare(
					"INSERT INTO UprawnieniObywatele (okreg, idObywatela, glosDoSenatu, glosDoSejmu) VALUES (?,?,?,?);"
			);
			INSERT_CANDIDATE = session.prepare(
					"INSERT INTO Okregi(okreg, idKandydata,imie, nazwisko) VALUES (?,?,?,?);"
			);
			DELETE_ALL_FROM_USERS = session.prepare("TRUNCATE users;");
		} catch (Exception e) {
			throw new BackendException("Could not prepare statements. " + e.getMessage() + ".", e);
		}

		logger.info("Statements prepared");
	}

	public String selectAll() throws BackendException {
		StringBuilder builder = new StringBuilder();
		BoundStatement bs = new BoundStatement(SELECT_ALL_FROM_USERS);

		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		for (Row row : rs) {
			String rcompanyName = row.getString("companyName");
			String rname = row.getString("name");
			int rphone = row.getInt("phone");
			String rstreet = row.getString("street");

			builder.append(String.format(USER_FORMAT, rcompanyName, rname, rphone, rstreet));
		}

		return builder.toString();
	}

	public void upsertUser(String companyName, String name, int phone, String street) throws BackendException {
		BoundStatement bs = new BoundStatement(INSERT_INTO_USERS);
		bs.bind(companyName, name, phone, street);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
		}

		logger.info("User " + name + " upserted");
	}

	public void deleteAll() throws BackendException {
		BoundStatement bs = new BoundStatement(DELETE_ALL_FROM_USERS);

		try {
			session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a delete operation. " + e.getMessage() + ".", e);
		}

		logger.info("All users deleted");
	}

	protected void finalize() {
		try {
			if (session != null) {
				session.getCluster().close();
			}
		} catch (Exception e) {
			logger.error("Could not close existing cluster", e);
		}
	}

}
