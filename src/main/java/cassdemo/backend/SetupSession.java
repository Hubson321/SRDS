package cassdemo.backend;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import java.io.IOException;
import java.util.UUID;

import java.util.List;

import cassdemo.ObjectsModel.Candidate;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SetupSession {

    private static final Logger logger = LoggerFactory.getLogger(BackendSession.class);
    private static PreparedStatement INSERT_CITIZEN;
    private static PreparedStatement INSERT_CANDIDATE_PARLIAMENT;
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
            INSERT_CANDIDATE_PARLIAMENT = session.prepare(
                    "INSERT INTO SejmWyniki(idKandydata,okreg, imie, nazwisko, votes) VALUES (?,?,?,?, ?);"
            );
            INSERT_CANDIDATE_SENATE = session.prepare(
                    "INSERT INTO SenatWyniki(idKandydata,okreg, imie, nazwisko, votes) VALUES (?,?,?,?, ?);"
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
            List<Candidate> parliamentList = mapper.readValue(
                    SetupSession.class.getResourceAsStream("/parliament.json"),
                    new TypeReference<List<Candidate>>() {}
            );
            List<Candidate> senateList = mapper.readValue(
                    SetupSession.class.getResourceAsStream("/senate.json"),
                    new TypeReference<List<Candidate>>() {}
            );
            prepareCandidatesList(parliamentList, true);
            prepareCandidatesList(senateList, false);

        }catch(IOException e){
            e.printStackTrace();
        }
    }

    private void prepareCandidatesList(List<Candidate> candidates, Boolean ifParliament){
        Integer counter = 0;
        Integer areaNum = 1;
        Integer areaFactor;
        // W zależności do której izby przygotowujemy liste inny współczynnik. Dla Senatu mamy 500
        // kandydatów, a dla Sejmu 1000 kandydatów. Wszędzie po 50 okręgów.
        if(ifParliament){
            areaFactor = 30;
        }else{
            areaFactor = 10;
        }
        for (Candidate candidate : candidates) {
            // umieszczanie kolejnych kandydatów w następnym okręgu wyborczym. Kazdy okręg po 10 kandydatów
            // łącznie 50 okregów spośród 500 kandydatów
            if(counter % areaFactor == 0){
                areaNum += 1;
            }
            String name = candidate.getName();
            String surname = candidate.getSurname();
            UUID candidateId = UUID.randomUUID();

            try {
                if (ifParliament){
                    insertCandidateParliament(areaNum, candidateId, name, surname);
                }else{
                    insertCandidateSenate(areaNum, candidateId, name, surname);
                }
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

    private void insertCandidateParliament(Integer areaNum, UUID candidateId, String name, String surname) throws  BackendException{
        BoundStatement bs = new BoundStatement(INSERT_CANDIDATE_PARLIAMENT);
        bs.bind(candidateId, areaNum,name, surname, 0);

        try {
            session.execute(bs);
        } catch (Exception e) {
            throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
        }
    }
    private void insertCandidateSenate(Integer areaNum, UUID candidateId, String name, String surname) throws  BackendException{
        BoundStatement bs = new BoundStatement(INSERT_CANDIDATE_SENATE);
        bs.bind(candidateId, areaNum, name, surname, 0);

        try {
            session.execute(bs);
        } catch (Exception e) {
            throw new BackendException("Could not perform an upsert. " + e.getMessage() + ".", e);
        }
    }
}