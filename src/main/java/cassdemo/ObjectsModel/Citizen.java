package cassdemo.ObjectsModel;

import java.util.UUID;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Citizen {
    private Integer areaId;
    private UUID citizenId;
    private Boolean voiceToParliament;
    private Boolean voiceToSenate;

    public Citizen(UUID _citizenId, Integer _areaId) {
        this.citizenId = _citizenId;
        this.areaId = _areaId;
        this.voiceToParliament = false;
        this.voiceToSenate = false;
    }
}