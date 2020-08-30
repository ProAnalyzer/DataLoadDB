import java.io.*;
import java.sql.*;
import java.time.LocalTime;

public class SqlInsertSequential {

    public static void main(String[] args) {
        String jdbcURL = "jdbc:postgresql://localhost:5432/";
        String username = "postgres";
        String password = "postgres";

        String csvFilePath = "EmpFinal.csv";

        int batchSize = 10;

        Connection connection = null;

        try {

            connection = DriverManager.getConnection(jdbcURL, username, password);
            connection.setAutoCommit(false);

            String sql = "INSERT INTO EmployeeEngineerJAVA " +
                    "(First_Name,Middle_Name,Last_Name,Age_of_Emp,Salary,Email,Phone_Number,Address,Description,Country) " +
                    "VALUES (?,?,?,?,?,?,?,?,?,?)";
            PreparedStatement statement = connection.prepareStatement(sql);

            BufferedReader lineReader = new BufferedReader(new FileReader(csvFilePath));
            String lineText ;

            int count = 0;

            lineReader.readLine(); // skip header line
            LocalTime startTime = LocalTime.now();
            System.out.println(startTime);
            while ((lineText = lineReader.readLine()) != null) {
                String[] data = lineText.split(",");
                String First_Name = data[0];
                String Middle_Name  = data[1];
                String Last_Name  = data[2];
                String Age_of_Emp = data[3];
                String Salary = data[4];
                String Email = data[5];
                String Phone_Number = data[6];
                String Address = data[7];
                String Description = data[8];
                String Country = data.length == 10 ? data[9] : "";


                statement.setString(1,First_Name );
                statement.setString(2, Middle_Name);
                statement.setString(3, Last_Name);

                int sqlAgeOfEmp = Integer.parseInt(Age_of_Emp);
                statement.setInt(4, sqlAgeOfEmp);

                float sqlSalary = Float.parseFloat(Salary);
                statement.setFloat(5, sqlSalary);

                statement.setString(6, Email);

                int sqlPhoneNumber = Integer.parseInt(Phone_Number);
                statement.setInt(7, sqlPhoneNumber);

                statement.setString(8, Address);
                statement.setString(9, Description);
                statement.setString(10, Country);

                statement.addBatch();

                if (count % batchSize == 0) {
                    statement.executeLargeBatch();
                    LocalTime endTime = LocalTime.now();
                    System.out.println(endTime);
                    System.out.println("\n");
                    System.out.println(endTime.getSecond()-startTime.getSecond());
                }
            }

            lineReader.close();

            // execute the remaining queries
            statement.executeBatch();

            connection.commit();
            connection.close();

        } catch (IOException ex) {
            System.err.println(ex);
        } catch (SQLException ex) {
            ex.printStackTrace();

            try {
                connection.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            catch (NullPointerException e){
                System.out.println("RollBack Null");
            }
        }
    }
}
