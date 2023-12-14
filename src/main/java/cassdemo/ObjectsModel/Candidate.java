public class Candidate {
    private String name;
    private String surname;

    public Candidate(String _name, String _surname){
        this.name = _name;
        this.surname = _surname
    }

    public String getName(){
        return this.name;
    }

    public String getSurname(){
        return this.surname;
    }
}
