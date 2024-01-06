package cassdemo.backend;

import cassdemo.ObjectsModel.Candidate;
import cassdemo.ObjectsModel.Citizen;
import cassdemo.ObjectsModel.Votes;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

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
    private static PreparedStatement UPDATE_CANDIDATE_PARLIAMENT;
    private static PreparedStatement UPDATE_CANDIDATE_SENATE;
    private static PreparedStatement GET_CITIZEN_CONSTITUENCY;
    private static PreparedStatement GET_CITIZENS;
    private static PreparedStatement DELETE_CITIZEN;
    private static PreparedStatement UPDATE_CITIZEN_PARLIAMENT;
    private static PreparedStatement UPDATE_CITIZEN_SENATE;
    private static PreparedStatement GET_CANDIDATES_PARLIAMENT;
    private static PreparedStatement GET_CANDIDATES_SENATE;
    private static PreparedStatement GET_CANDIDATE_PARLIAMENT_VOTES;
    private static PreparedStatement GET_CANDIDATE_SENATE_VOTES;

    private static final String USER_FORMAT = "- %-10s  %-16s %-10s %-10s\n";
    // private static final SimpleDateFormat df = new
    // SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public enum VotingType {
        PARLIAMENT,
        SENATE
    }

    private Integer getRandomNumber(int lowerBound, int upperBound) {
        Random random = new Random();
        int randomNumber = random.nextInt(upperBound - lowerBound + 1) + lowerBound;
        return randomNumber;
    }

    private List <Candidate> senateCandidates;
    private List <Candidate> parliamentCandidates;
    private List <Votes> candidateFinalResult;

	private Integer getGaussianRandomNumber(int lowerBound, int upperBound) {
        Random random = new Random();
        int firstAreaID = lowerBound;
        int lastAreaID = upperBound;

        double mean = (lowerBound + upperBound) / 2.0;
		double variance = Math.pow(upperBound - lowerBound, 2) / 12.0;
		double stddev = Math.sqrt(variance);

        // Rozklad normalny
        double u1 = 1.0 - random.nextDouble();
        double u2 = 1.0 - random.nextDouble();
        double randStdNormal = Math.sqrt(-2.0 * Math.log(u1)) * Math.sin(2.0 * Math.PI * u2);
        double randNormal = mean + stddev * randStdNormal;
        int randomNumber = (int) Math.round(Math.min(Math.max(randNormal, firstAreaID), lastAreaID));

        return randomNumber;
    }

    public BackendSession() {
        // taka sama definicja w drugim miejscu
        ObjectMapper mapper = new ObjectMapper();
        try {
            this.parliamentCandidates = mapper.readValue(
                SetupSession.class.getResourceAsStream("/parliament.json"),
                new TypeReference < List < Candidate >> () {}
            );
            this.senateCandidates = mapper.readValue(
                SetupSession.class.getResourceAsStream("/senate.json"),
                new TypeReference < List < Candidate >> () {}
            );

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void prepareStatements() throws BackendException {
        try {
            GET_CITIZEN_CONSTITUENCY = session.prepare("SELECT okreg, idobywatela, glosdosenatu, glosdosejmu FROM uprawnieniobywatele WHERE okreg = ?;");
            GET_CITIZEN = session.prepare("SELECT okreg, idObywatela, glosDoSejmu, glosDoSenatu FROM uprawnieniObywatele WHERE okreg = ? AND idObywatela = ?");
            GET_CITIZENS = session.prepare("SELECT okreg, idobywatela, glosdosenatu, glosdosejmu FROM uprawnieniobywatele;");
            DELETE_CITIZEN = session.prepare("DELETE FROM uprawnieniObywatele WHERE okreg = ? AND idobywatela = ?;");
            UPDATE_CITIZEN_PARLIAMENT = session.prepare("UPDATE UprawnieniObywatele SET glosDoSejmu = ? WHERE okreg = ? AND idobywatela = ?;");
            UPDATE_CITIZEN_SENATE = session.prepare("UPDATE UprawnieniObywatele SET glosDoSenatu = ? WHERE okreg = ? AND idobywatela = ?;");
            GET_CANDIDATES_PARLIAMENT = session.prepare("SELECT idkandydata, okreg, imie, nazwisko, votes FROM sejmWyniki WHERE okreg = ?;");
            GET_CANDIDATES_SENATE = session.prepare("SELECT idkandydata, okreg, imie, nazwisko, votes FROM senatWyniki WHERE okreg = ?;");

            GET_CANDIDATE_PARLIAMENT_VOTES = session.prepare("SELECT votes FROM sejmWyniki WHERE idkandydata = ? AND okreg = ?;");
            GET_CANDIDATE_SENATE_VOTES = session.prepare("SELECT votes FROM senatWyniki WHERE idkandydata = ? AND okreg = ?;");

            UPDATE_CANDIDATE_PARLIAMENT = session.prepare("UPDATE SejmWyniki SET votes = votes + 1 WHERE idKandydata = ? AND okreg = ? " +
                "AND imie = ? AND nazwisko = ?");

            UPDATE_CANDIDATE_SENATE = session.prepare("UPDATE SenatWyniki SET votes = votes + 1 WHERE idKandydata = ? AND okreg = ? " +
                "AND imie = ? AND nazwisko = ?");

            GET_FINAL_RESULT_PARLIAMENT = session.prepare("SELECT imie, nazwisko, votes FROM SejmWyniki;");
            GET_FINAL_RESULT_SENATE = session.prepare("SELECT imie, nazwisko, votes FROM SenatWyniki;");

        } catch (Exception e) {
            throw new BackendException("[prepareStatements] Could not prepare statements. " + e.getMessage() + ".", e);
        }

        logger.info("Statements prepared");
    }

    //        TODO: invoke this function after election has ended
    public void displayFinalResults(PreparedStatement tableQuery) throws BackendException {
        System.out.println("Wyniki do Sejmu: \n");
        prepareResults(GET_FINAL_RESULT_PARLIAMENT);
        System.out.println("Wyniki do Senatu: \n");
        prepareResults(GET_FINAL_RESULT_SENATE);
    }

    public Citizen getCitizen(Integer areaID, UUID citizenID) throws BackendException {
        BoundStatement bs = new BoundStatement(GET_CITIZEN);
        ResultSet rs = null;
        Row row = null;

        try {
            bs.bind(areaID, citizenID);
            rs = session.execute(bs);
            row = rs.one();
        } catch (Exception e) {
            throw new BackendException("[getCitizen] Could not perform a query. " + e.getMessage() + ".", e);
        }
        if (row != null) {
            Citizen citizen = new Citizen(citizenID, areaID);
            Boolean voiceToParliament = row.getBool("glosDoSejmu");
            Boolean voiceToSenate = row.getBool("glosDoSenatu");

            citizen.setVoiceToParliament(voiceToParliament);
            citizen.setVoiceToSenate(voiceToSenate);

            return citizen;
        }

        return null;
    }

    public Citizen getRandomCitizen() throws BackendException {
		// do sprawdzenia przy wiekszej liczbie obywateli, czy bedzie koniecznosc (?)
		Random random = new Random();
		int randomArea = random.nextInt(50) + 1;
        BoundStatement bs = new BoundStatement(GET_CITIZEN_CONSTITUENCY);
		bs.bind(randomArea);
        ResultSet rs = null;
        try {
            rs = session.execute(bs);
            List < Row > rows = rs.all();

            if (!rows.isEmpty()) {
                int citizenIndex = random.nextInt(rows.size());
                Row randomCitizenRow = rows.get(citizenIndex);

                Integer areaID = randomCitizenRow.getInt("okreg");
                UUID citizenID = randomCitizenRow.getUUID("idobywatela");
                Boolean senateVote = randomCitizenRow.getBool("glosdosenatu");
                Boolean parliamentVote = randomCitizenRow.getBool("glosdosejmu");

                Citizen randomCitizen = new Citizen(citizenID, areaID);
                randomCitizen.setVoiceToParliament(parliamentVote);
                randomCitizen.setVoiceToSenate(senateVote);

                return randomCitizen;
            } else {
                System.out.println("[getRandomCitizen] rows are empty!");
            }
        } catch (Exception e) {
            throw new BackendException("[getRandomCitizen] Could not perform a query. " + e.getMessage() + ".", e);
        }
        return null;
    }

    public void deleteCitizen(Integer areaID, UUID citizenID) throws BackendException {
        BoundStatement bs = new BoundStatement(DELETE_CITIZEN);
        ResultSet rs = null;
        bs.bind(areaID, citizenID);

        try {
            rs = session.execute(bs);
        } catch (Exception e) {
            throw new BackendException("[deleteCitizen] Could not perform a query. " + e.getMessage() + ".", e);
        }
    }

    public Candidate getRandomCandidate(VotingType votingType, Integer areaID) throws BackendException {

        BoundStatement bs = null;

        if (votingType == VotingType.PARLIAMENT) {
            bs = new BoundStatement(GET_CANDIDATES_PARLIAMENT);
        } else {
            bs = new BoundStatement(GET_CANDIDATES_SENATE);
        }

        ResultSet rs = null;
        try {
            bs.bind(areaID);
            rs = session.execute(bs);
            List <Row> rows = rs.all();

            if (!rows.isEmpty()) {
                Random random = new Random();
                int candidateIndex = random.nextInt(rows.size());
                Row randomCandidateRow = rows.get(candidateIndex);

                UUID candidateID = randomCandidateRow.getUUID("idkandydata");
                Integer CandidateAreaID = randomCandidateRow.getInt("okreg");
                String candidateName = randomCandidateRow.getString("imie");
                String candidateSurname = randomCandidateRow.getString("nazwisko");

                Candidate randomCandidate = new Candidate(candidateName, candidateSurname);
                randomCandidate.setCandidateId(candidateID);
                randomCandidate.setAreaId(CandidateAreaID);

                return randomCandidate;
            } else {
                System.out.println("[getRandomCandidate] rows are empty!");
            }
        } catch (Exception e) {
            throw new BackendException("[getRandomCandidate] Could not perform a query. " + e.getMessage() + ".", e);
        }
        return null;
    }

	public Candidate getRandomGaussianCandidate(VotingType votingType, Integer areaID) throws BackendException {
        BoundStatement bs = null;
		int candidateIndex = 1;
        if (votingType == VotingType.PARLIAMENT) {
            bs = new BoundStatement(GET_CANDIDATES_PARLIAMENT);
        } else {
            bs = new BoundStatement(GET_CANDIDATES_SENATE);
        }

        ResultSet rs = null;
        try {
            bs.bind(areaID);
            rs = session.execute(bs);
            List < Row > rows = rs.all();

            if (!rows.isEmpty()) {
                // ID w oparciu o wszystkich z danego okregu
                // wartosci generowane wykorzystujac rozklad normalny
				// 20 kandydatow w jednym okregu do sejmu, 10 do senatu
				if (votingType == VotingType.PARLIAMENT) {
					candidateIndex = getGaussianRandomNumber(1, 20);
				} else {
					candidateIndex = getGaussianRandomNumber(1, 10);
				}
                
                Row randomCandidateRow = rows.get(candidateIndex - 1);

                UUID candidateID = randomCandidateRow.getUUID("idkandydata");
                Integer CandidateAreaID = randomCandidateRow.getInt("okreg");
                String candidateName = randomCandidateRow.getString("imie");
                String candidateSurname = randomCandidateRow.getString("nazwisko");

                Candidate randomCandidate = new Candidate(candidateName, candidateSurname);
                randomCandidate.setCandidateId(candidateID);
                randomCandidate.setAreaId(CandidateAreaID);

                return randomCandidate;
            } else {
                System.out.println("[getRandomGaussianCandidate] rows are empty!");
            }
        } catch (Exception e) {
            throw new BackendException("[getRandomGaussianCandidate] Could not perform a query. " + e.getMessage() + ".", e);
        }
        return null;
    }

    public Long getCandidateVotes(Candidate candidate, VotingType votingType) throws BackendException {
        BoundStatement bs = null;
        Long candidateVotes = 0l;
        if (votingType == VotingType.PARLIAMENT) {
            bs = new BoundStatement(GET_CANDIDATE_PARLIAMENT_VOTES);
        } else {
            bs = new BoundStatement(GET_CANDIDATE_SENATE_VOTES);
        }

        bs.bind(candidate.getCandidateId(), candidate.getAreaId());

        try {
            ResultSet rs = session.execute(bs);
            candidateVotes = rs.one().getLong("votes");
            if (candidateVotes != null)
                return candidateVotes;
        } catch (Exception e) {
            throw new BackendException("[getCandidateVotes] Could not perform a query." + e.getMessage() + ".", e);
        }

        return candidateVotes;
    }

    public void voteParliament(Citizen citizen) throws BackendException {
        Candidate candidate = null;

        // oddanie w 99% glosu w poprawnym okregu wyborczym przeznaczonym dla obywatela
        Random random = new Random();
        double probability = 0.99;
        double randomValue = random.nextDouble(); // pobieramy wartosc od [0,1)

        if (randomValue < probability) {
			candidate = getRandomGaussianCandidate(VotingType.PARLIAMENT, citizen.getAreaId());
        } else {
            // z zakresu od 1 do 50 - zakres okregow wyborczych
            Integer randomAreaID = getRandomNumber(1, 50);
            candidate = getRandomGaussianCandidate(VotingType.PARLIAMENT, randomAreaID);
        }

        while (candidate.getAreaId() != citizen.getAreaId()) {
            randomValue = random.nextDouble();
            if (randomValue < probability) {
                candidate = getRandomGaussianCandidate(VotingType.PARLIAMENT, citizen.getAreaId());
            } else {
                // z zakresu od 1 do 50 - zakres okregow wyborczych
                Integer randomAreaID = getRandomNumber(1, 50);
                candidate = getRandomGaussianCandidate(VotingType.PARLIAMENT, randomAreaID);
            }
        }

        BoundStatement bs = new BoundStatement(UPDATE_CANDIDATE_PARLIAMENT);

        bs.bind(candidate.getCandidateId(),
            candidate.getAreaId(),
            candidate.getName(),
            candidate.getSurname()
        );

        // System.out.println("----------------------voteParliament----------------------");
        // System.out.println("candidateID: " + candidate.getCandidateId());
        // System.out.println("citizenID: " + citizen.getCitizenId());
        // System.out.println("areaID: " + candidate.getAreaId());
        // System.out.println("----------------------------------------------------------");

        if (candidate.getCandidateId() != null) {
            BoundStatement updateCitizenVote = new BoundStatement(UPDATE_CITIZEN_PARLIAMENT);
            updateCitizenVote.bind(true, citizen.getAreaId(), citizen.getCitizenId());
            BoundStatement updateParliamentCandidate = new BoundStatement(UPDATE_CANDIDATE_PARLIAMENT);
            updateParliamentCandidate.bind(
                candidate.getCandidateId(),
                candidate.getAreaId(),
                candidate.getName(),
                candidate.getSurname()
            );
            try {
                session.execute(updateCitizenVote);
                session.execute(updateParliamentCandidate);
            } catch (Exception e) {
                throw new BackendException("[voteParliament] Could not perform a query. " + e.getMessage() + ".", e);
            }
        }
    }

    public void voteSenate(Citizen citizen) throws BackendException {
        Candidate candidate = null;

        // oddanie w 99% glosu w poprawnym okregu wyborczym przeznaczonym dla obywatela
        Random random = new Random();
        double probability = 0.99;
        double randomValue = random.nextDouble(); // pobieramy wartosc od [0,1)

        if (randomValue < probability) {
			candidate = getRandomGaussianCandidate(VotingType.SENATE, citizen.getAreaId());
        } else {
            // z zakresu od 1 do 50 - zakres okregow wyborczych
            Integer randomAreaID = getRandomNumber(1, 50);
            candidate = getRandomGaussianCandidate(VotingType.SENATE, randomAreaID);
        }

        while (candidate.getAreaId() != citizen.getAreaId()) {
            randomValue = random.nextDouble();
            if (randomValue < probability) {
                candidate = getRandomGaussianCandidate(VotingType.SENATE, citizen.getAreaId());
            } else {
                // z zakresu od 1 do 50 - zakres okregow wyborczych
                Integer randomAreaID = getRandomNumber(1, 50);
                candidate = getRandomGaussianCandidate(VotingType.SENATE, randomAreaID);
            }
        }

        BoundStatement bs = new BoundStatement(UPDATE_CANDIDATE_SENATE);

        bs.bind(candidate.getCandidateId(),
            candidate.getAreaId(),
            candidate.getName(),
            candidate.getSurname()
        );

        // System.out.println("------------------------voteSenate------------------------");
        // System.out.println("candidateID: " + candidate.getCandidateId());
        // System.out.println("citizenID: " + citizen.getCitizenId());
        // System.out.println("areaID: " + candidate.getAreaId());
        // System.out.println("----------------------------------------------------------");

        if (candidate.getCandidateId() != null) {
            BoundStatement updateCitizenVote = new BoundStatement(UPDATE_CITIZEN_SENATE);
            updateCitizenVote.bind(true, citizen.getAreaId(), citizen.getCitizenId());
            BoundStatement updateParliamentCandidate = new BoundStatement(UPDATE_CANDIDATE_SENATE);
            updateParliamentCandidate.bind(
                candidate.getCandidateId(),
                candidate.getAreaId(),
                candidate.getName(),
                candidate.getSurname()
            );
            try {
                session.execute(updateCitizenVote);
                session.execute(updateParliamentCandidate);
            } catch (Exception e) {
                throw new BackendException("[voteSenate] Could not perform a query. " + e.getMessage() + ".", e);
            }
        }
    }

    public void voting() throws BackendException {

        Citizen citizen = getRandomCitizen();

        if (!citizen.getVoiceToParliament()) {
            voteParliament(citizen);
        }

        if (!citizen.getVoiceToSenate()) {
            voteSenate(citizen);
        }

        Citizen actualCitizen = getCitizen(citizen.getAreaId(), citizen.getCitizenId());

        if (actualCitizen.getVoiceToParliament() && actualCitizen.getVoiceToSenate()) {
			try {
				deleteCitizen(actualCitizen.getAreaId(), actualCitizen.getCitizenId());
				// System.out.println("--------------------------voting--------------------------");
				// System.out.println("Deleted citizen.");
				// System.out.println("AreaID: " + actualCitizen.getAreaId());
				// System.out.println("CitizenID: " + actualCitizen.getCitizenId());
				// System.out.println("----------------------------------------------------------");
			} catch (Exception e) {
                throw new BackendException("[voteSenate] Could not posible to delete citizen. " + e.getMessage() + ".", e);
            }
        }
    }

    private void prepareResults(PreparedStatement tableQuery) throws BackendException {
        BoundStatement bs = new BoundStatement(tableQuery);
        ResultSet rs = null;

        try {
            rs = session.execute(bs);
        } catch (Exception e) {
            throw new BackendException("[printer] Could not perform a query. " + e.getMessage() + ".", e);
        }

        for (Row row: rs) {
            String name = row.getString("imie");
            String surname = row.getString("nazwisko");
            long votes = row.getLong("votes");
            Votes candidateVote = new Votes(name, surname, votes);
            this.candidateFinalResult.add(candidateVote);
        }
        if (this.candidateFinalResult.size() != 0) {
            this.candidateFinalResult.sort(Comparator.comparingLong(Votes::getVotes).reversed());
            printer();
        }

        
    }

    private void printer() {
        System.out.println("[ Imię ] - [ Nazwisko ] - [ Liczba głosów ]");
        System.out.println("===========================================");
        for (Votes candidateVote : this.candidateFinalResult) {
            System.out.printf("%-15s %-15s %-10d%n", candidateVote.getName(), candidateVote.getSurname(),
                    candidateVote.getVotes());
        }

        this.candidateFinalResult.clear();
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
