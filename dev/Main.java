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



	public static class Passenger {
        	public boolean passengerCommand() {
            		System.out.println(
                    	"Passenger, what would you like to do?\n1. Request a ride\n2. Check trip records\n3. Go back");
            		return false;
        	}
        	public int getSqlCount(Statement stmt, String table) throws Exception {
            		String sql = "SELECT Count(*) as Total FROM " + table + ";";
            		ResultSet rs = stmt.executeQuery(sql);
            		rs.next();
            		int count = rs.getInt("Total");
            		return count;
        	}
        	public boolean checkexist(String station) {
            		try{
                       	String sql = "SELECT Count(*) as Total FROM taxi_stop WHERE name = '"+station +"';";
            			Connection con = connect();
            			PreparedStatement stmt = con.prepareStatement(sql);
          			ResultSet rs = stmt.executeQuery(sql);
           			rs.next();
           	 		int count = rs.getInt("Total");
           	 		con.close();
            			if (count!=0)
            			{
            				return true;
            			}	
            			return false;
                        
                   	}
                 	catch(Exception e)
                   	{  
                      		System.out.println("Error in connecting to database");
                   	}
                   	return false;
        	}
        	public boolean passenger_login() {
            		boolean askCommand = true;
            		while (true) {
                		if (askCommand) {
                    			askCommand = passengerCommand();
                		}
                		System.out.println("Please enter [1-3]");
                		String command = getInput();
                		switch (command) {
                    			case "1":
                       	 		askCommand = request_a_ride();
                       	 		break;
                    			case "2":
                       	 		askCommand = check_trip_record();
                       		 	break;
                    			case "3":
                       	 		return true;
                    			default:
                       	 		System.out.println("[ERROR] Invalid input.");
                		}
            		}
        	}
        
        
                public boolean request_a_ride() {
            
                	int passenger_id=-1;
                	int passenger_no=-1;
                
                	do{
                		System.out.println("Please enter your ID.");
                		String str = getInput();
                		try{
                        		passenger_id=Integer.parseInt(str);
                        
                   		}
                		catch(Exception e)
                   		{  
                     			System.out.println("[ERROR] Not vaild input.");
                   		}
                   		if(passenger_id<0)
                   		{
                   			System.out.println("[ERROR] Not vaild input.");
                   		}
                   		
                  
                	}while(passenger_id<0);
                
                	do{
                		System.out.println("Please enter the number of passengers.");
                		String str = getInput();
                		try{
                       	 	passenger_no=Integer.parseInt(str);
                        
                   		}
                		catch(Exception e)
                   		{  
                     			System.out.println("[ERROR] Not vaild input.");
                   		}
                   		if((passenger_no>8)||(passenger_no<1))
                   		{
                   			System.out.println("[ERROR] Not vaild input.");
                   		}
                   
                	}while((passenger_no>8)||(passenger_no<1));
                	String end;
                	String start;
                	do{
                		System.out.println("Please enter the start location.");
                 		start = getInput();
                 	
                 	
                		while(checkexist(start)==false)
                		{
                			System.out.println("[ERROR] Station not exist.");
                 			start = getInput();
                 		
                		}
                		System.out.println("Please enter the destination.");
                		end = getInput();
                		while(checkexist(end)==false)
                		{
                			System.out.println("[ERROR] Station not exist.");
                 			end = getInput();
                		}
               	 	if(start.equalsIgnoreCase(end)==true)
                		{
               		 	System.out.println("[ERROR] Start location and destination should not be the same.");
                
               		}
               	 }while(start.equalsIgnoreCase(end)==true);
                	System.out.println("Please enter the model. (Press enter to skip)");
                	String model = getInput();
                	if (model=="")
                	{
                		model= "NULL";
                	}
                	
                	int min_years = -1;
                	do{
                		System.out.println("Please enter the minimum driving years of driver.  (Press enter to skip)");
                		String min_years_str = getInput();
                		if(min_years_str=="")
                		{
                			min_years =0;
                			break;
                		}
                		try{
                        		min_years=Integer.parseInt(min_years_str);
                   		}
                		catch(Exception e)
                   		{  
                        		System.out.println("[ERROR] Not vaild input.");
                   		}
                   		if(min_years<0)
                   		{
                   			System.out.println("[ERROR] Not vaild input.");
                   		}
            		}while(min_years<0);
                	int id =getid();
                	int no_drivers=check_driver_condition(model,passenger_no,min_years);   //check how many drivers can take the request
                	if(no_drivers==0)
                	{
                		System.out.println("[ERROR] No drivers are able to take the request.");
                	}
                	else
                	{
                		if(send_request(id,passenger_id,start,end,model,passenger_no,min_years)==true)
                		{
		        		System.out.printf("Your request is placed. %d drivers are able to take the request.\n",no_drivers);
                		}
                	}
                
                	return true;
           
        	}
        	public int check_driver_condition(String model,int passengers,int driving_years){  //check how many drivers can take the request
        
            		try {
            			PreparedStatement ps ;
            			Connection con = connect();
            			String sql;
            			if(model == "NULL")
            			{
            				sql = "SELECT COUNT(*) AS Total FROM driver INNER JOIN vehicle ON driver.vehicle_id = vehicle.id WHERE driver.driving_years >= "+Integer.toString(driving_years) +" AND vehicle.seats >= "+Integer.toString(passengers) +";";
            				ps = con.prepareStatement(sql);
		    		}
            			else{
            				sql = "SELECT COUNT(*) AS Total FROM driver INNER JOIN vehicle ON driver.vehicle_id = vehicle.id WHERE driver.driving_years >= "+Integer.toString(driving_years) +" AND vehicle.seats >= "+Integer.toString(passengers) +" AND vehicle.model LIKE '%"+ model +"%';";
            				ps = con.prepareStatement(sql);
            			}
            			ResultSet rs = ps.executeQuery(sql);
                		rs.next();
                		int count = rs.getInt("Total");
                		con.close();
                		return count;
            		} catch (Exception e) {
                		System.out.println("[ERROR] SQL Error in send request.");
                		System.out.println(e.toString()); // need to comment
            		}
            		return -1;
        	}
        	public boolean send_request(int id,int passenger_id,String start_location,String destination,String model,int passengers,int driving_years){  //send request to server
            		try {
            			Connection con = connect();
            			String sql = "insert into request values(?,?,?,?,?,?,?,?);";
                		PreparedStatement ps = con.prepareStatement(sql);
                		ps.setInt(1, id);
                		ps.setInt(2, passenger_id);
                		ps.setString(3, start_location);
                		ps.setString(4, destination);
                		ps.setString(5, model);
                		ps.setInt(6, passengers);
                		ps.setInt(7, 0);
                		ps.setInt(8, driving_years);
                		ps.executeUpdate();
                		con.close();
                		return true;
            		} catch (Exception e) {
                		System.out.println("[ERROR] SQL Error in send request.");
                		System.out.println(e.toString()); // need to comment
            		}
            		return false;
        	}
        	public int getid() {   //get the request id
            		try {
                		Connection con = connect();
                		Statement stmt = con.createStatement();
                		int count =getSqlCount(stmt, "request")+1;
                		con.close();
                		return count;
            		} catch (Exception e) {
                		System.out.println("[ERROR] SQL Error in Check data.");
                		System.out.println(e.toString()); // need to comment
            		}
            		return -1;
        	}
        
        	public boolean checking_trip_record(int passenger_id,String start,String end,String destination){
            		try {
            			PreparedStatement ps ;
            			Connection con = connect();
            			String sql;
            			sql = "SELECT trip.id, driver.name, vehicle.id, vehicle.model, trip.start_time, trip.end_time, trip.fee, trip.start_location, trip.destination FROM trip INNER JOIN driver ON trip.driver_id = driver.id INNER JOIN vehicle ON driver.vehicle_id = vehicle.id WHERE trip.destination= '"+destination+"' AND trip.passenger_id= " +Integer.toString(passenger_id) +" AND trip.start_time >= '"+start+" 00:00:00' AND trip.end_time <= '"+end+" 23:59:59' ORDER BY trip.start_time DESC;";
            			ps = con.prepareStatement(sql);
            
            			ResultSet rs = ps.executeQuery(sql);
            			if(!rs.isBeforeFirst())
            			{
            				System.out.println("No record founds.");
            			}
            			else{
            
            				System.out.println("Trip_id, Driver Name, Vehicle ID, Vehicle Model, Start, End, Fee, Start Location, Destination");
            				while(rs.next()){
            	
            				System.out.printf("%d, %s, %s, %s, %s, %s, %d, %s, %s\n",rs.getInt(1),rs.getString(2),rs.getString(3),rs.getString(4),rs.getString(5),rs.getString(6),rs.getInt(7),rs.getString(8),rs.getString(9));
            				}
            				con.close();
            			}
            		return true;
            		} catch (Exception e) {
                			System.out.println("[ERROR] SQL Error in send request.");
                			System.out.println(e.toString()); // need to comment
            		}
            		return false;
        	}
        	public int checkdatevalid(String start, String end) {   //get the request id
            		try{
            			SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");
      				java.util.Date d1 = sdformat.parse(start);
      				java.util.Date d2 = sdformat.parse(end);
      				if(d1.compareTo(d2) > 0) {
      					System.out.println("[Error} start date occurs after end date.");
         				return -1;
         				
      				}
      				return 1;
            		
            		} catch (Exception e) {
            				System.out.println("[Error} Invalid date");
                			return -1;
            		}
        	}
        	
		public boolean check_trip_record() {
                	int passenger_id=-1;
                	do{
                		System.out.println("Please enter your ID.");
                		String str = getInput();
                		try{
                        		passenger_id=Integer.parseInt(str);
                        
                   		}
                		catch(Exception e)
                   		{  
                     			System.out.println("[ERROR] Not vaild input.");
                   		}
                   		if(passenger_id<0)
                   		{
                   			System.out.println("[ERROR] Not vaild input.");
                   		}
                   
                	}while(passenger_id<0);
                	 
                	String pattern = "([0-9]{4})-(0?[1-9]|1[0-2])-(0?[1-9]|[12][0-9]|3[01])";
                	String start;
                	String end;
                	int datevalid=1;
                	do{
                		do{
                			System.out.println("Please enter the start date.");
                			start = getInput();
                			if(start.matches(pattern))
                			{
                				datevalid =1;
                			}
                			else
                			{
                				datevalid =0;
                				System.out.println("[ERROR] Not vaild input.");
                			}
                		}while(datevalid== 0);
                	
                	
                		do{
                			System.out.println("Please enter the end date.");
                			end = getInput();
                			if(end.matches(pattern))
                			{
                				datevalid =1;
                			}
                			else
                			{
                				datevalid =0;
                				System.out.println("[ERROR] Not vaild input.");
                			}
                		}while(datevalid== 0);
                		if(checkdatevalid(start,end)==1)
                		{
                			datevalid =1;
                		
                		}
                		else
                		{
                			datevalid =0;
                		}
                		
                	}while(datevalid==0);
                	System.out.println("Please enter the destination.");
                	String destination = getInput();
                	while(checkexist(destination)==false)
                	{
                		System.out.println("[ERROR] Station not exist.");
                		destination = getInput();
                	}	
                
                	if(checking_trip_record(passenger_id,start,end,destination)==false)
                	{
                		System.out.println("Error in check a trip");
                	}
		       return true;
        	}
        
        }

//passenger class end








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
		     Passenger passengertmp = new Passenger();
		     askRole = passengertmp.passenger_login();
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
