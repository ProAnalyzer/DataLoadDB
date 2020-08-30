/*

import java.io.*;
import java.net.URL;
import java.sql.*;
import java.time.LocalTime;
import java.util.Properties;

public class CreateTable {

    public static void main(String[] args) throws Exception {
        ClassLoader loader = CreateTable.class.getClassLoader();
        URL urls = loader.getResource("DBconn.properties");

        try {
            File filePath = new File(urls.toURI());
            System.out.println("Class Path : " + filePath);
        } catch (NullPointerException e) {
            System.out.println("Sorry! We cannot find your properties file for making connection");
        }
        try (InputStream input = loader.getResourceAsStream("DBconn.properties")) {

            Properties prop = new Properties();

            if (input == null) {
                System.out.println("Sorry, unable to find DBConn.properties");
                return;
            }

            prop.load(input);

            String url = prop.getProperty("url");
            String user = prop.getProperty("user");
            String password = prop.getProperty("password");

            System.out.println("url : " + url);
            System.out.println("user : " + user);
            System.out.println("password : " + password);


            try (Connection conn = DriverManager.getConnection(url, user, password); Statement statement = conn.createStatement()) {
                if (conn != null) {
                    System.out.println("Connected to the database!");
                    statement.execute(createTableSQL);
                    System.out.println(LocalTime.now());
                    */
/*try {
                        BufferedReader bReader = new BufferedReader(new FileReader("Employee10000.csv"));

                        while (bReader != null) {
                            String read ;
                            try {
                                read = bReader.readLine();
                                if (read != null)
                                {
                                    String[] array = read.split(",+");
                                    for(String result:array)
                                    {
                                        System.out.println(result );
                                    }
                                }
                            } catch (IOException ex) {
                                ex.printStackTrace();
                            }
                            finally
                            {
                                if (bReader == null)
                                {
                                    bReader.close();
                                }
                            }
                        }
                    } catch (FileNotFoundException ex) {
                        ex.printStackTrace();
                    }*//*

                    //statement.execute(createTableSQL);
                } else {
                    System.out.println("Failed to make connection!");
                }

            } catch (SQLException e) {
                System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage());
            } catch (Exception e) {
                e.printStackTrace();
            }


        } catch (IOException ex) {
            ex.printStackTrace();
        }

    }

    private static final String createTableSQL = "CREATE TABLE EmployeeEngineer " +
            "(Emp_ID SERIAL PRIMARY KEY ," +
            " First_Name TEXT, " +
            " Middle_Name TEXT, " +
            " Last_Name TEXT, " +
            " Age_of_Emp INT, " +
            " Salary FLOAT, " +
            " Email VARCHAR(50), " +
            " Phone_Number BIGINT, " +
            " Address VARCHAR(80), " +
            " Description TEXT, " +
            " Country VARCHAR(50))";

*/
/*    private  static final String query = "INSERT INTO employee " +
            "(First_Name,Middle_Name,Last_Name,Date_of_Birth,Age_of_Emp,Start_Date,Salary,City,Phone_Number,Address,Description)"+
                "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";*//*

}

*/
