package cassdemo.ObjectsModel;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class Candidate {
    public String name;
    public String surname;

    public Candidate(String _name, String _surname){
        this.name = _name;
        this.surname = _surname;
    }
}
