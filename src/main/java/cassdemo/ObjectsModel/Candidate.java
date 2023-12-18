package cassdemo.ObjectsModel;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Candidate {
    private String name;
    private String surname;

    public Candidate(String _name, String _surname){
        this.name = _name;
        this.surname = _surname;
    }
}
