public class Employee {

    private String First_Name;
    private String Middle_Name;
    private String Last_Name;
    private Integer Age_of_Emp;
    private String Salary;
    private String Email;
    private String Phone_Number;
    private String Address;
    private String Description;
    private String Country;

    public Employee(String First_Name, String Middle_Name, String Last_Name,
                    Integer Age_of_Emp, String Salary, String Email, String Phone_Number,
                    String Address, String Description, String Country) {
        this.First_Name = First_Name ;
        this.Middle_Name = Middle_Name ;
        this.Last_Name = Last_Name ;
        this.Age_of_Emp = Age_of_Emp ;
        this.Salary = Salary ;
        this.Email = Email ;
        this.Phone_Number = Phone_Number ;
        this.Address = Address ;
        this.Description = Description ;
        this.Country = Country ;
    }


    public String getFirst_Name() {
        return First_Name;
    }
    public void setFirst_Name(String First_Name) {
        this.First_Name = First_Name;
    }


    public String getMiddle_Name() {
        return Middle_Name;
    }
    public void setMiddle_Name(String Middle_Name) {
        this.Middle_Name = Middle_Name;
    }


    public String getLast_Name() {
        return Last_Name;
    }
    public void setLast_Name(String Last_Name) {
        this.Last_Name = Last_Name;
    }


    public Integer getAge_of_Emp() {
        return Age_of_Emp;
    }
    public void setAge_of_Emp(Integer Age_of_Emp) {
        this.Age_of_Emp = Age_of_Emp;
    }

    public String getSalary() {
        return Salary;
    }
    public void setSalary(String Salary) {
        try {
            int i = Integer.parseInt((Salary.trim()));
        }
        catch (NumberFormatException e ){
            System.out.println("Salary Format not Correct");
        }
        this.Salary = Salary;
    }


    public String getEmail() {
        return Email;
    }
    public void setEmail(String Email) {
        this.Email = Email;
    }


    public String getPhone_Number () {
        return Phone_Number;
    }
    public void setPhone_Number(String Phone_Number) {
        try {
            int i = Integer.parseInt((Phone_Number.trim()));
        }
        catch (NumberFormatException e ){
            System.out.println("Phone Number Format not Correct");
        }
        this.Phone_Number = Phone_Number;
    }


    public String getAddress() {
        return Address;
    }
    public void setAddress(String Address) {
        this.Address = Address;
    }


    public String getDescription() {
        return Description;
    }
    public void setDescription(String Description) {
        this.Description = Description;
    }


    public String getCountry() {
        return Country;
    }
    public void setCountry(String Country) {
        this.Country = Country;
    }
}