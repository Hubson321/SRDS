package cassdemo.ObjectsModel;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class Votes {
    public String name;
    public String surname;
    Long votes;

    public Votes(String _name, String _surname, Long _votes) {
        this.name = _name;
        this.surname = _surname;
        this.votes = _votes;
    }
}
