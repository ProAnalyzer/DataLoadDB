import sun.awt.EmbeddedFrame;
import sun.invoke.empty.Empty;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.*;

public class Parallel {
    private static void insertBatchPostings(Collection<Employee>employeeCollection) throws Exception {
        String jdbcURL = "jdbc:postgresql://localhost:5432/";
        String username = "postgres";
        String password = "postgres";

        String csvFilePath = "EmpFinal.csv";

        Connection connection = null;
        connection = DriverManager.getConnection(jdbcURL, username, password);
        String sql = "INSERT INTO EmployeeEngineer " +
                "(First_Name,Middle_Name,Last_Name,Age_of_Emp,Salary,Email,Phone_Number,Address,Description,Country) " +
                "VALUES (?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement statement = connection.prepareStatement(sql);
        BufferedReader lineReader = new BufferedReader(new FileReader(csvFilePath));
        String lineText = null;
        lineText = lineReader.readLine();
        String[] data = lineText.split(",");
        String First_Name = data[0];
        String Middle_Name = data[1];
        String Last_Name = data[2];
        String Age_of_Emp = data[3];
        String Salary = data[4];
        String Email = data[5];
        String Phone_Number = data[6];
        String Address = data[7];
        String Description = data[8];
        String Country = data.length == 10 ? data[9] : "";


        statement.setString(1, First_Name);
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
    }

    private static final int insertionThreads = 4;

    private static void insertBatchPostingsThreaded(final Collection<Collection<Employee>> batches) {
        ExecutorService pool = Executors.newFixedThreadPool(insertionThreads);
        Collection<Future> futures = new ArrayList<Future>(batches.size());

        for (final Collection<Employee> batch : batches) {
            Callable c = new Callable() {
                public Object call() throws Exception {
                    insertBatchPostings(batch);
                    return null;
                }
            };
 //So we submit each batch to the pool, and keep a note of its Future so we can check it later.

            futures.add(pool.submit(c));
        }

 //Pool is running, indicate that no further work will be submitted to it.

        pool.shutdown();

 //Check all the futures for problems.

        for (Future f : futures) {
            try {
                f.get();
            } catch (InterruptedException ex) {
                System.out.println(System.err);
            } catch (ExecutionException ex) {
                pool.shutdownNow();
            }
        }
    }

    public static void main(String[] args) {
        insertBatchPostingsThreaded();
        //I don't understand what should be the return for insertBatchPostingsThreaded()
    }
}
