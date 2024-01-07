package cassdemo.ObjectsModel;

import java.util.UUID;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Citizen {
    private Integer areaID;
    private UUID citizenID;
    private Boolean voiceToParliament;
    private Boolean voiceToSenate;

    public Citizen(UUID _citizenID, Integer _areaID) {
        this.citizenID = _citizenID;
        this.areaID = _areaID;
        this.voiceToParliament = false;
        this.voiceToSenate = false;
    }
}