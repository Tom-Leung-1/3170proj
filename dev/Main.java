import java.util.*;

import javax.naming.spi.DirStateFactory.Result;

import java.sql.*;
import java.io.*;
import java.text.*;

public class Main {

    public static class Administrator {
        public boolean adminCommand() {
            System.out.println(
                    "Administrator, what would you like to do?\n1. Create tables\n2. Delete tables\n3. Load data\n4. Check data\n5. Go back");
            return false;
        }

        public boolean createTable() {
            System.out.print("Processing...");
            try {
                Connection con = connect();
                Statement stmt = con.createStatement();
                String createVehicle = "CREATE TABLE `vehicle` (" + "`id` CHAR(6) NOT NULL,"
                        + "`model` CHAR(30) NOT NULL," + "`seats` INT UNSIGNED NOT NULL CHECK (`seats` > 0),"
                        + "PRIMARY KEY (`id`));";
                String createDriver = "CREATE TABLE `driver` (" + "`id` INT NOT NULL CHECK (`id` > 0),"
                        + "`name` CHAR(30) NOT NULL," + "`vehicle_id` CHAR(6) NOT NULL,"
                        + "`driving_years` INT UNSIGNED NOT NULL," + // in csv some data are 0
                        "PRIMARY KEY (`id`)," + "FOREIGN KEY (`vehicle_id`) REFERENCES `vehicle`(`id`));";
                String createPassenger = "CREATE TABLE `passenger` (" + "`id` INT NOT NULL CHECK (`id` > 0),"
                        + "`name` CHAR(30) NOT NULL," + "PRIMARY KEY (`id`));";
                String createTaxiStop = "CREATE TABLE `taxi_stop` (" + "`name` CHAR(20) NOT NULL,"
                        + "`location_x` INT NOT NULL," + "`location_y` INT NOT NULL," + "PRIMARY KEY (`name`));";
                String createTrip = "CREATE TABLE `trip` (" + "`id` INT NOT NULL CHECK (`id` > 0),"
                        + "`driver_id` INT NOT NULL CHECK (`driver_id` > 0),"
                        + "`passenger_id` INT NOT NULL CHECK (`passenger_id` > 0),"
                        + "`start_location` CHAR(20) NOT NULL," + "`destination` CHAR(20) NOT NULL,"
                        + "`start_time` DATETIME NOT NULL," + "`end_time` DATETIME NOT NULL,"
                        + "`fee` INT NOT NULL CHECK (`fee` > 0)," + "PRIMARY KEY (`id`),"
                        + "FOREIGN KEY (`driver_id`) REFERENCES `driver`(`id`),"
                        + "FOREIGN KEY (`passenger_id`) REFERENCES `passenger`(`id`),"
                        + "FOREIGN KEY (`start_location`) REFERENCES `taxi_stop`(`name`),"
                        + "FOREIGN KEY (`destination`) REFERENCES `taxi_stop`(`name`));";
                String createRequest = "CREATE TABLE `request` (" + "`id` INT NOT NULL CHECK (`id` > 0),"
                        + "`passenger_id` INT NOT NULL CHECK (`passenger_id` > 0),"
                        + "`start_location` CHAR(20) NOT NULL," + "`destination` CHAR(20) NOT NULL,"
                        + "`model` CHAR(30) NOT NULL," + "`passengers` INT NOT NULL CHECK (`passengers` > 0)," + // set>0
                        "`taken` TINYINT NOT NULL," + // set 1 taken or 0 not taken
                        "`driving_years` INT NOT NULL," + // in csv some data are 0, didn't add check condition
                        "PRIMARY KEY (`id`)," + "FOREIGN KEY (`passenger_id`) REFERENCES `passenger`(`id`),"
                        + "FOREIGN KEY (`start_location`) REFERENCES `taxi_stop`(`name`),"
                        + "FOREIGN KEY (`destination`) REFERENCES `taxi_stop`(`name`));";
                stmt.executeUpdate(createVehicle);
                stmt.executeUpdate(createDriver);
                stmt.executeUpdate(createPassenger);
                stmt.executeUpdate(createTaxiStop);
                stmt.executeUpdate(createTrip);
                stmt.executeUpdate(createRequest);
                System.out.println("Done! Tables are created!");
                return true;
            } catch (SQLException e) {
                System.out.println("\n[ERROR] SQL Error in Create tables.");
                System.out.println(e.toString());
            }
            return false;
        }

        public boolean deleteTable() {
            System.out.print("Processing...");
            try {
                Connection con = connect();
                Statement stmt = con.createStatement();
                String dropTrip = "Drop table trip;";
                String dropRequest = "Drop table request;";
                String dropDriver = "Drop table driver;";
                String dropVehicle = "Drop table vehicle;";
                String dropPassenger = "Drop table passenger;";
                String dropTaxiStop = "Drop table taxi_stop;";
                stmt.executeUpdate(dropTrip);
                stmt.executeUpdate(dropRequest);
                stmt.executeUpdate(dropDriver);
                stmt.executeUpdate(dropVehicle);
                stmt.executeUpdate(dropPassenger);
                stmt.executeUpdate(dropTaxiStop);
                System.out.println("Done! Tables are deleted!");
                return true;
            } catch (SQLException e) {
                System.out.println("\n[ERROR] SQL Error in Delete tables.");
                System.out.println(e.toString());
            }
            return false;
        }

        public List<String> getRecords(String path) throws Exception {
            Scanner sc = new Scanner(new File(path));
            sc.useDelimiter("\n");
            List<String> records = new ArrayList<String>();
            while (sc.hasNext()) {
                records.add(sc.next());
            }
            sc.close();
            return records;
        }

        public boolean loadData() {
            while (true) {
                try {
                    System.out.println("Please enter the folder path");
                    String path = getInput(); // ../test_data (might change)
                    System.out.print("Processing...");
                    File folder = new File(path);
                    File[] listOfFiles = folder.listFiles();
                    Map<String, List<String>> map = new HashMap<String, List<String>>();
                    for (File file : listOfFiles) {
                        if (file.isFile()) {
                            List<String> records = getRecords(path + "/" + file.getName());
                            map.put(file.getName(), records);
                        }
                    }
                    // order (due to foreign key) taxi_stop > passenger > vehicle > driver > trip >
                    // request
                    Connection con = connect();
                    PreparedStatement ps = null;
                    if (map.containsKey("taxi_stops.csv")) {
                        List<String> records = map.get("taxi_stops.csv");
                        String sql = "insert into taxi_stop values(?,?,?)";
                        for (String r : records) {
                            r = r.replace("\n", "").replace("\r", "");
                            String[] elements = r.split(",");
                            ps = con.prepareStatement(sql);
                            ps.setString(1, elements[0]);
                            ps.setInt(2, Integer.parseInt(elements[1]));
                            ps.setInt(3, Integer.parseInt(elements[2]));
                            ps.executeUpdate();
                        }
                    }
                    if (map.containsKey("passengers.csv")) {
                        List<String> records = map.get("passengers.csv");
                        String sql = "insert into passenger values(?,?)";
                        for (String r : records) {
                            r = r.replace("\n", "").replace("\r", "");
                            String[] elements = r.split(",");
                            ps = con.prepareStatement(sql);
                            ps.setInt(1, Integer.parseInt(elements[0]));
                            ps.setString(2, elements[1]);
                            ps.executeUpdate();
                        }
                    }
                    if (map.containsKey("vehicles.csv")) {
                        List<String> records = map.get("vehicles.csv");
                        String sql = "insert into vehicle values(?,?,?)";
                        for (String r : records) {
                            r = r.replace("\n", "").replace("\r", "");
                            String[] elements = r.split(",");
                            ps = con.prepareStatement(sql);
                            ps.setString(1, elements[0]);
                            ps.setString(2, elements[1]);
                            ps.setInt(3, Integer.parseInt(elements[2]));
                            ps.executeUpdate();
                        }
                    }
                    if (map.containsKey("drivers.csv")) {
                        List<String> records = map.get("drivers.csv");
                        String sql = "insert into driver values(?,?,?,?)";
                        for (String r : records) {
                            r = r.replace("\n", "").replace("\r", "");
                            String[] elements = r.split(",");
                            ps = con.prepareStatement(sql);
                            ps.setInt(1, Integer.parseInt(elements[0]));
                            ps.setString(2, elements[1]);
                            ps.setString(3, elements[2]);
                            ps.setInt(4, Integer.parseInt(elements[3]));
                            ps.executeUpdate();
                        }
                    }
                    if (map.containsKey("trips.csv")) {
                        List<String> records = map.get("trips.csv");
                        String sql = "insert into trip values(?,?,?,?,?,?,?,?)";
                        for (String r : records) {
                            r = r.replace("\n", "").replace("\r", "");
                            String[] elements = r.split(",");
                            ps = con.prepareStatement(sql);
                            ps.setInt(1, Integer.parseInt(elements[0]));
                            ps.setInt(2, Integer.parseInt(elements[1]));
                            ps.setInt(3, Integer.parseInt(elements[2]));
                            ps.setString(4, elements[5]); // start location
                            ps.setString(5, elements[6]); // destination
                            ps.setTimestamp(6, toSqlTimestamp(elements[3])); // start time
                            ps.setTimestamp(7, toSqlTimestamp(elements[4])); // end time
                            ps.setInt(8, Integer.parseInt(elements[7]));
                            ps.executeUpdate();
                        }
                    }
                    System.out.println("Data is loaded!");
                    return true;
                } catch (NullPointerException e) {
                    System.out.println("[ERROR] folder path doesn't exist");
                } catch (Exception e) {
                    System.out.println("[ERROR] SQL Error in Load data.");
                    System.out.println(e.toString()); // need to comment
                }
            }
        }

        public String getSqlCount(Statement stmt, String table) throws Exception {
            String sql = "SELECT Count(*) as Total FROM " + table + ";";
            ResultSet rs = stmt.executeQuery(sql);
            rs.next();
            int count = rs.getInt("Total");
            return Integer.toString(count);
        }

        public boolean checkData() {
            try {
                Connection con = connect();
                Statement stmt = con.createStatement();
                System.out.println("Numbers of records in each table:");
                System.out.println("Vehicle: " + getSqlCount(stmt, "vehicle"));
                System.out.println("Passenger: " + getSqlCount(stmt, "passenger"));
                System.out.println("Driver: " + getSqlCount(stmt, "driver"));
                System.out.println("Trip: " + getSqlCount(stmt, "trip"));
                System.out.println("Request: " + getSqlCount(stmt, "request"));
                System.out.println("Taxi Stop: " + getSqlCount(stmt, "taxi_stop"));
                return true;
            } catch (Exception e) {
                System.out.println("[ERROR] SQL Error in Check data.");
                System.out.println(e.toString()); // need to comment
            }
            return false;
        }

        public boolean workFlow() {
            boolean askCommand = true;
            while (true) {
                if (askCommand) {
                    askCommand = adminCommand();
                }
                System.out.println("Please enter [1-5]");
                String command = getInput();
                switch (command) {
                    case "1":
                        askCommand = createTable();
                        break;
                    case "2":
                        askCommand = deleteTable();
                        break;
                    case "3":
                        askCommand = loadData();
                        break;
                    case "4":
                        askCommand = checkData();
                        break;
                    case "5":
                        return true;
                    default:
                        System.out.println("[ERROR] Invalid input.");
                }
            }
        }
    }

    public static java.sql.Timestamp toSqlTimestamp(String date) throws Exception {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-DD HH:mm:ss");
        java.util.Date parsedDate = dateFormat.parse(date);
        // System.out.println(parsedDate);
        java.sql.Timestamp sqlParsedDate = new java.sql.Timestamp(parsedDate.getTime());
        return sqlParsedDate;
    }

    public static Connection connect() {
        String dbAddress = "jdbc:mysql://projgw.cse.cuhk.edu.hk:2633/group17";
        String dbUsername = "Group17";
        String dbPassword = "3170group17";

        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection(dbAddress, dbUsername, dbPassword);
            // System.out.println("connected");
        } catch (ClassNotFoundException e) {
            System.out.println("[ERROR]: Java MySQL DB Driver not found!!");
            System.out.println(e.toString());
            System.exit(0);
        } catch (SQLException e) {
            System.out.println(e);
        }
        return con;
    }

    static boolean greetings() {
        System.out.println(
                "Welcome! Who are you?\n1. An Administrator\n2. A passenager\n3. A driver\n4. A manager\n5. None of the above");
        return false;
    }

    static String getInput() {
        Scanner sc = new Scanner(System.in);
        return sc.nextLine();
    }

    public static void main(String[] args) {
        boolean askRole = true;
        while (true) { // 5 -> close program?
            if (askRole) {
                askRole = greetings();
            }
            System.out.println("Please enter [1-4]");
            String role = getInput();
            switch (role) {
                case "1":
                    Administrator admin = new Administrator();
                    askRole = admin.workFlow();
                    break;
                case "2":

                    break;
                case "3":

                    break;
                case "4":

                    break;
                default:
                    System.out.println("[ERROR] Invalid input.");
            }
        }
    }
}