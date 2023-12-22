package cassdemo.backend;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import java.io.File;
import java.util.List;

import cassdemo.ObjectsModel.Candidate;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SetupSession {

    private static final Logger logger = LoggerFactory.getLogger(BackendSession.class);
    private static PreparedStatement INSERT_CITIZEN;
    private static PreparedStatement INSERT_CANDIDATE_PARLIEMENT;
    private static  PreparedStatement INSERT_CANDIDATE_SENATE;
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

    public void setupCandidatesAndCitizens() {
//        this.prepareCitizenSetup();
        this.prepareCandidateSetup();
    }

    public void prepareStatements() throws BackendException {
        try {
            INSERT_CITIZEN = session.prepare(
                    "INSERT INTO UprawnieniObywatele (okreg, idObywatela, glosDoSenatu, glosDoSejmu) VALUES (?,?,?,?);"
            );
            INSERT_CANDIDATE_PARLIEMENT = session.prepare(
                    "INSERT INTO SejmWyniki(idKandydata,okreg, imie, nazwisko, votes) VALUES (?,?,?,?);"
            );
            INSERT_CANDIDATE_SENATE = session.prepare(
                    "INSERT INTO SenatWyniki(idKandydata,okreg, imie, nazwisko, votes) VALUES (?,?,?,?);"
            );
        } catch (Exception e) {
            throw new BackendException("Could not prepare statements. " + e.getMessage() + ".", e);
        }

        logger.info("Statements prepared");
    }

    private void prepareCitizenSetup(){
        Integer areaNum = 1;
        //  Integer ALL_USERS = 28000000;
        Integer ALL_USERS = 1000000;
        // for tests: 1000000 / 40
        for (int i = 1; i <= ALL_USERS; i++) {
            UUID userId = UUID.randomUUID();
            // jeden z 41 okregów wyborczych
            // 28 mln / 41 ~ 682927 - tylu obywateli per okręg, tymczasowo mln glosujących
            if( i % 25000 == 0){
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
        Integer areaNum = 1;
        ObjectMapper mapper = new ObjectMapper();
        try{
            List<Candidate> candidateList = mapper.readValue(
                    SetupSession.class.getResourceAsStream("/candidates.json"),
                    new TypeReference<List<Candidate>>() {}
            );

            Integer counter = 0;
            for (Candidate candidate : candidateList) {
                // umieszczanie kolejnych kandydatów w następnym okręgu wyborczym. Kazdy okręg po 28 kandydatów
                if(counter % 25 == 0){
                    areaNum += 1;
                }
                String name = candidate.getName();
                String surname = candidate.getSurname();
                UUID candidateId = UUID.randomUUID();

                try {
                    insertCandidate(areaNum, candidateId, name, surname);
                    counter++;
                } catch (Exception e) {
                    System.err.println("Error inserting user: " + e.getMessage());
                }
            }
        }catch(IOException e){
            e.printStackTrace();
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
        bs.bind(areaNum, candidateId, name, surname);

        try {
            session.execute(bs);
        } catch (Exception e) {
            throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
        }
    }
}