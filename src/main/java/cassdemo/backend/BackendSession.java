package cassdemo.backend;

import cassdemo.ObjectsModel.Candidate;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

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
    private static PreparedStatement GET_CITIZEN;
    private static PreparedStatement GET_CANDIDATE_PARLIAMENT;
    private static PreparedStatement GET_CANDIDATE_SENATE;
    private static PreparedStatement UPDATE_CITIZEN;
    private static PreparedStatement UPDATE_CANDIDATE_SENATE;
    private static PreparedStatement UPDATE_CANDIDATE_PARLIAMENT;

    private static final String USER_FORMAT = "- %-10s  %-16s %-10s %-10s\n";
    // private static final SimpleDateFormat df = new
    // SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private List<Candidate> senateCandidates;
    private List<Candidate> parliamentCandidates;

    public BackendSession() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            this.parliamentCandidates = mapper.readValue(
                    SetupSession.class.getResourceAsStream("/parliament.json"),
                    new TypeReference<List<Candidate>>() {
                    }
            );
            this.senateCandidates = mapper.readValue(
                    SetupSession.class.getResourceAsStream("/senate.json"),
                    new TypeReference<List<Candidate>>() {
                    }
            );

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void prepareStatements() throws BackendException {
        try {
            SELECT_ALL_FROM_USERS = session.prepare("SELECT * FROM users;");
            INSERT_INTO_USERS = session
                    .prepare("INSERT INTO users (companyName, name, phone, street) VALUES (?, ?, ?, ?);");
            DELETE_ALL_FROM_USERS = session.prepare("TRUNCATE users;");
            GET_FINAL_RESULT_PARLIAMENT = session.prepare("SELECT imie, nazwisko, votes FROM SejmWyniki ORDER BY DESC;");
            GET_FINAL_RESULT_SENATE = session.prepare("SELECT imie, nazwisko, votes FROM SenatWyniki ORDER BY " +
                    "DESC;");
            GET_CITIZEN = session.prepare("SELECT okreg, glosDoSenatu, glosDoSejmu FROM UprawnieniObywatele WHERE " +
                    "glosDoSenatu = false OR glosDoSejmu = false ORDER BY timestamp LIMIT 1;");
            GET_CANDIDATE_PARLIAMENT = session.prepare("SELECT idKandydata FROM SejmWyniki WHERE imie = ? AND " +
                    "nazwisko = ?;");
            GET_CANDIDATE_SENATE = session.prepare("SELECT idKandydata, okreg FROM SenatWyniki WHERE imie = ? AND " +
                    "nazwisko = ?;");
            UPDATE_CITIZEN = session.prepare("UPDATE UprawnieniObywatele SET glosDoSejmu = ?, glosDoSenatu = ? WHERE " +
                    "idObywatela = ?;");
            UPDATE_CANDIDATE_SENATE = session.prepare("UPDATE SenatWyniki SET votes = votes + 1 WHERE idKandydata = " +
                    "?;");
            UPDATE_CANDIDATE_PARLIAMENT = session.prepare("UPDATE SejmWyniki SET votes = votes + 1 WHERE idKandydata " +
                    "= " +
                    "?;");

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

    public void voting() throws BackendException {
        BoundStatement bs = new BoundStatement(GET_CITIZEN);
        try {
            ResultSet rs = session.execute(bs);
            Row row = rs.one();
            if (row != null) {
                Integer citizenArea = row.getInt("okreg");
                UUID citizenID = row.getUUID("idObywatela");
                Boolean senateVote = row.getBool("glosDoSenatu");
                Boolean parliamentVote = row.getBool("glosDoSejmu");
                if (!senateVote) {
                    voteSenate(citizenArea, senateVote, parliamentVote, citizenID);
                }
                if (!parliamentVote) {
                    voteParliament(citizenArea, senateVote, parliamentVote, citizenID);
                }
            }
        } catch (Exception e) {
            throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
        }
    }

    private void voteSenate(Integer citzenArea, Boolean senateVote, Boolean parliamentVote, UUID citizenID) throws BackendException {
        Random random = new Random();
        BoundStatement senatePotentialCandidate = new BoundStatement(GET_CANDIDATE_SENATE);

        Integer candidateArea = 0;
        UUID candidateID = null;

        while (candidateArea != citzenArea) {
            int candidateIndex = random.nextInt(this.senateCandidates.size());
            Candidate candidate = this.senateCandidates.get(candidateIndex);
            senatePotentialCandidate.bind(candidate.getName(), candidate.getSurname());
            try {
                ResultSet rs = session.execute(senatePotentialCandidate);
                Row row = rs.one();
                if (row != null) {
                    candidateID = row.getUUID("idKandydata");
                    candidateArea = row.getInt("okreg");
                }

            } catch (Exception e) {
                throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
            }
        }
        if (candidateID != null) {
            BoundStatement updateCitizenVote = new BoundStatement(UPDATE_CITIZEN);
            updateCitizenVote.bind(parliamentVote, true, citizenID);
            BoundStatement updateSenateCandidate = new BoundStatement(UPDATE_CANDIDATE_SENATE);
            updateSenateCandidate.bind(candidateID);
            try {
                session.execute(updateCitizenVote);
                session.execute(updateSenateCandidate);
            } catch (Exception e) {
                throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
            }
        }
    }

    private void voteParliament(Integer citzenArea, Boolean senateVote, Boolean parliamentVote, UUID citizenID) throws BackendException {
        Random random = new Random();
        Integer candidateArea = 0;
        BoundStatement parliamentPotentialCandidate = new BoundStatement(GET_CANDIDATE_PARLIAMENT);
        UUID candidateID = null;

        while (candidateArea != citzenArea) {
            int candidateIndex = random.nextInt(this.parliamentCandidates.size());
            Candidate candidate = this.parliamentCandidates.get(candidateIndex);
            parliamentPotentialCandidate.bind(candidate.getName(), candidate.getSurname());
            try {
                ResultSet rs = session.execute(parliamentPotentialCandidate);
                Row row = rs.one();
                if (row != null) {
                    candidateID = row.getUUID("idKandydata");
                    candidateArea = row.getInt("okreg");
                }

            } catch (Exception e) {
                throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
            }
        }
        if (candidateID != null) {
            BoundStatement updateCitizenVote = new BoundStatement(UPDATE_CITIZEN);
            updateCitizenVote.bind(true, senateVote, citizenID);
            BoundStatement updateParliamentCandidate = new BoundStatement(UPDATE_CANDIDATE_PARLIAMENT);
            updateParliamentCandidate.bind(candidateID);
            try {
                session.execute(updateCitizenVote);
                session.execute(updateParliamentCandidate);
            } catch (Exception e) {
                throw new BackendException("Could not perform a query. " + e.getMessage() + ".", e);
            }
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
