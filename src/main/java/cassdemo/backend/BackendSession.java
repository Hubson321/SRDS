package cassdemo.backend;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

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
	private static PreparedStatement GET_FINAL_RESULT_PARLIAMENT;
	private static PreparedStatement GET_FINAL_RESULT_SENATE;

	private static final String USER_FORMAT = "- %-10s  %-16s %-10s %-10s\n";
	// private static final SimpleDateFormat df = new
	// SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private void prepareStatements() throws BackendException {
		try {
			SELECT_ALL_FROM_USERS = session.prepare("SELECT * FROM users;");
			INSERT_INTO_USERS = session
					.prepare("INSERT INTO users (companyName, name, phone, street) VALUES (?, ?, ?, ?);");
			DELETE_ALL_FROM_USERS = session.prepare("TRUNCATE users;");
			GET_FINAL_RESULT_PARLIAMENT = session.prepare("SELECT imie, nazwisko, votes FROM SejmWyniki ORDER BY DESC;");
			GET_FINAL_RESULT_SENATE = session.prepare("SELECT imie, nazwisko, votes FROM SenatWyniki ORDER BY " +
					"DESC;");
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

//	TODO: invoke this function after election has ended
	public void displayFinalResults(PreparedStatement tableQuery) throws BackendException {
		printer(GET_FINAL_RESULT_PARLIAMENT);
		printer(GET_FINAL_RESULT_SENATE);
	}

	private void printer(PreparedStatement tableQuery) throws BackendException {
		BoundStatement bs = new BoundStatement(tableQuery);
		ResultSet rs = null;

		try {
			rs = session.execute(bs);
		} catch (Exception e) {
			throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
		}

		for (Row row : rs) {
			String name = row.getString("imie");
			String surname = row.getString("nazwisko");
			long votes = row.getLong("votes");
			System.out.printf("%-15s %-15s %-10d%n", name, surname, votes);
		}
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
