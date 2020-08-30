import com.opencsv.CSVWriter;

import java.io.FileWriter;
import java.io.IOException;

public class CreateCSV {

    public static void main(String[] args) throws IOException {
        CSVWriter csvWriter = new CSVWriter(new FileWriter("Employee100001.csv"));
        String[] header = { "First_Name", "Middle_Name", "Last_Name",
                "Age_of_Emp", "Salary", "Email", "Phone_Number",
                "Address", "Description", "Country"};
        csvWriter.writeNext(header);
        for(int i =0; i<10000; i++)
            csvWriter.writeNext(new String[]{"Pronay", "Kumar", "Ghosh",
                    "22", "0000.00", "kumarpronayghosh@gmail.com", "6291081728",
                    "Baranagar", "Engineer", "India"});

        csvWriter.close();
    }
}

/*Your CSV file is in your project root path*/
