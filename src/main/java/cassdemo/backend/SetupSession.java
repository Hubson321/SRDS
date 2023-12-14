package cassdemo.backend;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import cassdemo.ObjectsModel.Candidate;

public class SetupSession {

    private static final Logger logger = LoggerFactory.getLogger(BackendSession.class);
    private static PrepareStatement INSERT_CITIZEN;
    private static PrepareStatement INSERT_CANDIDATE;
    private Session session;

    public SetupSession(String contactPoint, String keyspace) throws BackendException {

        Cluster cluster = Cluster.builder().addContactPoint(contactPoint).build();
        try {
            session = cluster.connect(keyspace);
        } catch (Exception e) {
            throw new BackendException("Could not connect to the cluster. " + e.getMessage() + ".", e);
        }
        prepareStatements();
    }

    private void setupCandidatesAndCitizens() throws BackendException {
        this.prepareCitizenSetup();
        this.prepareCandidateSetup();
    }

    private void prepareStatements() throws BackendException {
        try {
            INSERT_CITIZEN = session.prepare(
                    "INSERT INTO UprawnieniObywatele (okreg, idObywatela, glosDoSenatu, glosDoSejmu) VALUES (?,?,?,?);"
            );
            INSERT_CANDIDATE = session.prepare(
                    "INSERT INTO Okregi(okreg, idKandydata,imie, nazwisko) VALUES (?,?,?,?);"
            );
        } catch (Exception e) {
            throw new BackendException("Could not prepare statements. " + e.getMessage() + ".", e);
        }

        logger.info("Statements prepared");
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
        List<Candidate> candidateList = Arrays.asList(objectMapper.readValue(new File("src/main/resource/candidates" +
                ".json"),	Candidate[].class));
        Integer counter = 0;
        for (Candidate candidate : candidateList) {
            // umieszczanie kolejnych kandydatów w następnym okręgu wyborczym. Kazdy okręg po 28 kandydatów
            if(counter % 27 == 0){
                areaNum += 1;
            }
            String name = candidate.getName;
            String surname = candidate.getSurname();
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
}