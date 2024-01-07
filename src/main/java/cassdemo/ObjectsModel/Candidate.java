package cassdemo.ObjectsModel;

import java.util.UUID;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class Candidate {
    public String name;
    public String surname;
    private UUID candidateId;
    private Integer areaId;
    private Long votes;

    public Candidate(String _name, String _surname) {
        this.name = _name;
        this.surname = _surname;
        this.candidateId = null;
        this.areaId = 0;
        this.votes = 0l;
    }
}
