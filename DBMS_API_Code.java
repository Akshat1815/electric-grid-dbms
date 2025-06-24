import java.sql.Connection;
import java.sql.DriverManager;
// import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;


public class DBMS_API_Code {
    public static void main(String[] args) {
        String url = "jdbc:postgresql://localhost:5432/Electricity_Grid";
        String user = "postgres";
        String password = "******";

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            if (conn != null) {
                System.out.println("Connected to the database!");

                // Set search path to electricity_grid_management schema
                try (Statement setPathStmt = conn.createStatement()) {
                    setPathStmt.execute("SET search_path TO electricity_grid_management;");
                }

                // Get user input for Pin Code
                Scanner scanner = new Scanner(System.in);
                boolean flag = true;
                while(flag) {

                    printPrompt();
                    int inp = scanner.nextInt();
                    
                    switch(inp) {
                        case 1: SelectQuery.getActiveCustomersByPinCode(conn); break;
                        case 2: SelectQuery.getCountOfCustomerByPinCode(conn); break;
                        case 3: SelectQuery.getSubstationForAPinCode(conn); break;
                        case 4: SelectQuery.getLoadCapacityForPinCodes(conn); break;
                        case 5: SelectQuery.getActiveMeterForSubstationWithNFeeder(conn); break;
                        case 6: SelectQuery.getCountOfMeterForAState(conn); break;
                        case 7: SelectQuery.maintSchedWithFeederLoadCapacity(conn); break;
                        case 8: SelectQuery.getCustUnPaidBill(conn); break;
                        case 9: SelectQuery.consumpTrendByCustomerTypesPerYear(conn); break;
                        case 10: flag = false; break;

                    }
                }

                scanner.close();


            } else {
                System.out.println("Failed to make a connection!");
            }
        } catch (SQLException e) {
            System.out.println("SQL Error: " + e.getMessage());
        }
    }

    public static void printPrompt() {
        System.out.println("1) Retrieve active Customers for a Given Pin Code.");
        System.out.println("2) Count the number of Customers for all Pin Code.");
        System.out.println("3) Get Substation for a Given Pin_Code.");
        System.out.println("4) Get the Sum of electricity load capacity for all Pin Codes.");
        System.out.println("5) List active meters in substations with more than n feeders.");
        System.out.println("6) Retrieve the count of meters in a particular state, categorized by the type of customer.");
        System.out.println("7) Find all the total number of maintenance schedules of the substations which have at least 1 feeder with the total number of customers associated with it being more than its load capacity can handle. ");
        System.out.println("8) Find customers with unpaid bills over a certain amount.");
        System.out.println("9) To find the consumption trend of different customer types per year for the different electrical rates");
        System.out.println("10) Exit.");
    }

}


class SelectQuery {
    static Scanner scanner = new Scanner(System.in);

    // Method to retrieve active customers by Pin Code
    public static void getActiveCustomersByPinCode(Connection conn) throws SQLException {
        System.out.print("Enter Pin-Code: ");
        String pinCode = scanner.nextLine();
        System.out.println(pinCode);
        
        String sql = "SELECT Customer.Customer_ID, Customer.Customer_name, Customer.Phone_Number " +
                        "FROM Customer " +
                        "JOIN Pin_Code ON Customer.Pin_Code = Pin_Code.Pin_Code " +
                        "WHERE Pin_Code.Pin_Code ='"+pinCode+
                        "' AND Customer.Connection_Status = 'Connected';";
        
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);

        while(rs.next()) {
            String id = rs.getString("customer_id");
            String name = rs.getString("customer_name");
            String phone_number = rs.getString("phone_number");

            System.out.println(id + "\t" + name + "\t" + phone_number);
        }
    }

    // Count the number of customers for all Pin_Code
    public static void getCountOfCustomerByPinCode(Connection conn) throws SQLException {

        String sql = "SELECT customer.pin_code, COUNT(Customer.Customer_ID) AS Customer_Count " +
                     "FROM Customer " +
                     "GROUP BY customer.pin_code;"; 
        
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);

        while(rs.next()) {
            String pin_code = rs.getString("pin_code");
            int count = rs.getInt("customer_count");

            System.out.println(pin_code + "\t" + count);
        }

    }

    // Get Substation for a Given Pin_Code
    public static void getSubstationForAPinCode(Connection conn) throws SQLException {
        System.out.println("Enter Pin-Code: ");
        String pinCode = scanner.nextLine();

        String sql = "SELECT Substation.Substation_ID, Substation.area, Substation.status " +
                     "FROM Substation " + 
                     "JOIN Pin_Code ON Substation.Pin_Code = Pin_Code.Pin_Code " + 
                     "WHERE Pin_Code.Pin_Code = '" + pinCode + "';";
        
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);

        while(rs.next()) {
            String id = rs.getString("substation_id");
            String area = rs.getString("area");
            String status = rs.getString("status");

            System.out.println(id + "\t" + area + "\t" + status);
        }
    }

    // To find the Sum of electricity load capacity in a given Pin_Code
    public static void getLoadCapacityForPinCodes(Connection conn) throws SQLException {

        String sql = "SELECT Pin_Code.Pin_Code, SUM(Feeder.Capacity) AS Total_Capacity " + 
                     "FROM Feeder " + 
                     "JOIN Substation ON Feeder.Substation_ID= Substation.Substation_ID " + 
                     "JOIN Pin_Code ON Substation.Pin_Code = Pin_Code.Pin_Code " + 
                     "GROUP BY Pin_Code.Pin_Code;";
        
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);

        while(rs.next()) {
            String pinCode = rs.getString("pin_code");
            int capacity = rs.getInt("total_capacity");

            System.out.println(pinCode + "\t" + capacity);

        }

    }   

    // List active meters in substations with more than n feeders
    public static void getActiveMeterForSubstationWithNFeeder(Connection conn) throws SQLException{

        System.out.println("Enter n: ");
        String n = scanner.nextLine();
        // String nstr = Integer.toString(n);

        // String sql = "SELECT M.Meter_ID, M.Current_Reading, S.Substation_ID " + 
        //              "FROM Meter M " + 
        //              "JOIN Feeder F ON M.Feeder_ID = F.Feeder_ID " + 
        //              "JOIN Substation S ON F.Substation_ID = S.Substation_ID" + 
        //              "WHERE S.Substation_ID IN ( " + 
        //              "    SELECT Substation_ID " + 
        //              "    FROM Feeder " + 
        //              "    GROUP BY Substation_ID " + 
        //              "    HAVING COUNT(Feeder_ID) > " + nstr + 
        //              ") AND M.Status = 'Active';";

        String sql = "SELECT M.Meter_ID, M.Current_Reading, S.Substation_ID FROM Meter M JOIN Feeder F ON M.Feeder_ID = F.Feeder_ID JOIN Substation S ON F.Substation_ID = S.Substation_ID WHERE S.Substation_ID IN ( SELECT Substation_ID FROM Feeder GROUP BY Substation_ID HAVING COUNT(Feeder_ID) > " + n + ") AND M.Status = 'Active'";


        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);

        while(rs.next()) {
            String id = rs.getString("meter_id");
            int reading = rs.getInt("current_reading");
            String id2 = rs.getString("substation_id");
            
            System.out.println(id + "\t" + reading + "\t" + id2);
        }
    }

    // Retrieve the count of meters in a particular state, categorized by the type of customer.
    public static void getCountOfMeterForAState(Connection conn) throws SQLException {
        System.out.println("Enter State: ");
        String state = scanner.nextLine();


        String sql = "SELECT " + 
                        "COUNT(me.meter_id) as number_of_meters, ct.type_name " + 
                     "FROM  " + 
                        "meter as me " + 
                     "NATURAL JOIN  " + 
                        "Customer as cu " + 
                     "NATURAL JOIN " + 
                        "Customer_Type as ct " + 
                     "NATURAL JOIN " + 
                         "Pin_code as pc " + 
                     "WHERE " + 
                        "pc.state = '" + state +
                     "' GROUP BY " + 
                        "ct.type_name " + 
                     "ORDER BY " + 
                        "number_of_meters DESC;";


        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);

        while(rs.next()) {
            int no = rs.getInt("number_of_meters");
            String name = rs.getString("type_name");
            System.out.println(no + "\t" + name);
        }
    }

    // Find all the total number of maintenance schedules of the substations which have at least 1 feeder with the total number of customers associated with it being more than its load capacity can handle. 
    public static void maintSchedWithFeederLoadCapacity(Connection conn) throws SQLException {

        String tempsql = "SELECT avg(Bill.Total_Price/er.Electricity_Rate) " + 
                         "FROM Feeder AS fe " + 
                         "JOIN Substation AS sub ON  sub.substation_ID = fe.substation_ID " + 
                         "JOIN Meter ON Meter.Feeder_ID = fe .Feeder_ID " + 
                         "JOIN Bill ON Bill.Meter_ID= Meter.Meter_ID " + 
                         "JOIN Electricity_Rate AS er ON Bill.rate_ID = er.rate_ID ";

        Statement tempstmt = conn.createStatement();
        ResultSet temprs = tempstmt.executeQuery(tempsql);

        float avg = 0;
        if(temprs.next()) {
            avg =temprs.getFloat("avg");
        }
        // System.out.println(avg);

        String sql = "SELECT count(Maintenance_ID), substation_ID  " + 
                     "FROM Maintenance_Schedule   " + 
                     "WHERE Substation_ID IN (  " + 
                     "SELECT DISTINCT sub.Substation_ID  " + 
                     "FROM Substation AS sub  " + 
                     "LEFT JOIN Feeder ON sub.substation_ID = Feeder.substation_ID  " + 
                     "WHERE  Feeder.Feeder_ID IN (  " + 
                     "SELECT DISTINCT Feeder.Feeder_ID  " + 
                     "FROM Feeder  " + 
                     "LEFT JOIN Meter ON Feeder.Feeder_ID=Meter.Feeder_ID  " + 
                     "GROUP BY Feeder.Feeder_ID, Feeder.Load_Profile  " + 
                     "HAVING  count(Meter.Meter_ID)> (0.9*10*Feeder.Load_Profile)/" + avg + 
                     ")  " + 
                     ")  " + 
                     "GROUP BY Substation_ID;  ";

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);

        while(rs.next()) {
            int count = rs.getInt("count");
            String id = rs.getString("substation_id");

            System.out.println(count + "\t" + id);
        }
    }

    // Find customers with unpaid bills over a certain amount
    public static void getCustUnPaidBill(Connection conn) throws SQLException{
        System.out.print("Enter Amount: ");
        String amount = scanner.nextLine();

        String sql = "SELECT C.Customer_ID, C.Customer_Name, B.Total_Price, B.Billing_Date  " + 
        "FROM Customer C " + 
        "JOIN Meter M ON C.Customer_ID = M.Customer_ID  " + 
        "JOIN Bill B ON M.Meter_ID = B.Meter_ID  " + 
        "WHERE B.Payment_Status = 'Unpaid' AND B.Total_Price > " + amount +  "; ";

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);

        while(rs.next()) {
            String id = rs.getString("customer_id");
            String name = rs.getString("customer_name");
            int price = rs.getInt("total_price");
            String date = rs.getString("billing_date");

            System.out.println(id + "\t" + name + "\t" + price + "\t" + date);
        }



    }

    // To find the consumption trend of different customer types per year for the different electrical rates
    public static void consumpTrendByCustomerTypesPerYear(Connection conn) throws SQLException{
        
        String sql = "SELECT  avg(Bill.Total_Price/er.Electricity_Rate) , er.customer_type_ID AS ctp, er.electricity_rate AS rate, EXTRACT (YEAR FROM bill.Billing_Date) AS yr " + 
        "FROM Bill  " + 
        "JOIN Electricity_Rate AS er ON Bill.rate_ID = er.rate_ID " + 
        "GROUP BY er.customer_type_ID, er.electricity_rate, EXTRACT (YEAR FROM bill.Billing_Date) " + 
        "ORDER BY ctp,rate,yr;";

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);

        while(rs.next()) {
            float avg = rs.getFloat("avg");
            int id = rs.getInt("ctp");
            float rate = rs.getFloat("rate");
            int year = rs.getInt("yr");

            System.out.println(avg + "\t" + id + "\t" + rate + "\t" + year);
        }

    }

}