package com.assignment2;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;


/**
 * Creating cluster and session objects to connect to Cassandra and
 * Updating information on student table from keyspace datamodel
 */
public class App 
{
	/****
	 * This function checks the value of city name
	 * checks whether it's Cork and then changes it to Dublin
	 * and visa versa
	 * @param currentCity - value of current city
	 * @return new city value
	 */
	private static String checkCity(String currentCity)
	{
		String newCityValue = "Cork";
		if(currentCity.equalsIgnoreCase("Cork"))
			newCityValue = "Dublin";
		return newCityValue;
	}
	
    public static void main( String[] args )
    {
        //set required variables to connect to Cassandra
    	String serverIp = "127.0.0.1";
        String keyspace = "datamodel";
        String user = "cassandra";
        String password = "cassandra";
        
        //create cluster object
        Cluster cluster = Cluster.builder()
                .addContactPoints(serverIp)
                .withCredentials(user, password)
                .build();

        //create session object
        Session session = cluster.connect(keyspace);
       
        //view all entries for the student table statement
        String selectAll = "SELECT * FROM student";
        
        //get city value for the first student row
        String firstStudentCity = session.execute(selectAll).one().getString("student_city");
      //get student id for the first student entry
        String firstStudentId = session.execute(selectAll).one().getUUID("student_id").toString();
        
        //get new value for the city
        String newCityValue = checkCity(firstStudentCity);
        
        System.out.println("---------Original student table------------");
        //execute select all statement on student table and print information in console prior to update
        for (Row row : session.execute(selectAll)) {
            System.out.println(row.toString());
        }
        //update first student's city
        String cqlUpdate = "UPDATE student SET student_city = '"+newCityValue+"' WHERE student_id = "+firstStudentId;
        //execute update statement
        session.execute(cqlUpdate);
        
        System.out.println("---------Updated student table------------");
        
        //execute select statement on the tables again to view the updated results
        for (Row row : session.execute(selectAll)) {
            System.out.println(row.toString());
        }
        
        
        //close session
        session.close();
    }
}
