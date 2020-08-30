import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.ParseLong;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.constraint.StrRegEx;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanWriter;
import org.supercsv.io.ICsvBeanWriter;
import org.supercsv.prefs.CsvPreference;

public class CreateSuperCSV
{
    //Watch out for Exception in thread "main" java.lang.ExceptionInInitializerError
    private static List<Employee> employees = new ArrayList<Employee>();

    static
    {
        for (int i =0; i<10000; i++){
            employees.add(new Employee("Pronay", "Kumar", "Ghosh",22,"1111","kumarpronayghosh@gmail.com","885655545","Dunlop","SoftwareEngineer","India"));
        }
    }
    private static CellProcessor[] getProcessors()
    {
        final String emailRegex = "[a-z0-9\\._]+@[a-z0-9\\.]+";

        StrRegEx.registerMessage(emailRegex, "must be a valid email address");

        final CellProcessor[] processors = new CellProcessor[] {
                new NotNull(), // First_Name
                new Optional(), // Middle_name
                new NotNull(),  //Last_Name
                new NotNull(new ParseInt()),  //Age_Of_Emp
                new NotNull(new ParseInt()),  //salary
                new StrRegEx(emailRegex), // Email
                new NotNull(new ParseLong()), //PhoneNumber
                new NotNull(),  //Address
                new NotNull(), //Description
                new NotNull(), //Country
        };
        return processors;
    }

    public static void main(String[] args)
    {

        ICsvBeanWriter beanWriter = null;

        try
        {
            beanWriter = new CsvBeanWriter(new FileWriter("EmpFinal101.csv"), CsvPreference.STANDARD_PREFERENCE);
            final String[] header = new String[] {"First_Name", "Middle_Name", "Last_Name",
                    "Age_of_Emp", "Salary", "Email", "Phone_Number",
                    "Address", "Description", "Country"};

            final CellProcessor[] processors = getProcessors();

            // write the header
            beanWriter.writeHeader(header);

            // write the beans data
            for (Employee e : employees) {
                beanWriter.write(e, header, processors);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }  finally {
            try {
                beanWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}