import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import com.mysql.jdbc.PreparedStatement;

import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;
import javax.sound.sampled.Clip;

/**
 * @author Kostas Thanasis
 *
 */

public class DBManagement {
	// IMPORTANT!!! Change this values!!
	// UI WONT ASK FOR THOSE VALUES!!
	private static String host = "localhost";   
	private static String port = "3306";
	private static String database = "bdset2";
	private static String user = "root";
	private static String password = "59785978k";
	//---------------------------//
	
	
	public static void main(String[] args) {
		//sound related..
		Song song = new Song();
		Thread thread = new Thread(song);
		thread.start();
		
		boolean exit = false;
		Scanner in = new Scanner(System.in);
		
		System.out.println("Song playing: Gone with the wind soundtrack...");
		System.out.print("Press 1 or 2 to select between questions,\"start\" or \"stop\" for the song to start or stop respectively,and then hit enter: ");
		while (!exit) {
			
			
			String userInput = in.nextLine();
			
			while (!(userInput.equals("1") || userInput.equals("2")||userInput.toLowerCase().equals("start")||userInput.toLowerCase().equals("stop"))) {
				System.out.print("Please press 1 or 2 to select between questions,start or stop for the song to start or stop respectively,and then hit enter: ");
				userInput = in.nextLine();

			}

			if (userInput.equals("1")) {
				String query = getStatementQ1(); // Method to get a correct query for question 1

				if (!query.equals("")) {
					System.out.println(q1(query.toString())); // Run the q1 with the above query and print the result
				}

			} else if (userInput.equals("2")){
				// Stall for five seconds so user can read the output.
				System.out.println("In 5 seconds the answer for q2 will appear.");
				System.out.println("No user input is needed for this question.");
				try {
					TimeUnit.SECONDS.sleep(5);
				} catch (InterruptedException ie) {
					Thread.currentThread().interrupt();
				}
				System.out.println(q2());
				
			}else if (userInput.toLowerCase().equals("start")) {
				song.run();
				continue;
				
			}else if (userInput.toLowerCase().equals("stop")) {
				song.stop();
				continue;
			}

			System.out.print("Do you want to exit the programm? (y/n):");
			String exitCheck = in.nextLine();
			
			if (exitCheck.toLowerCase().contentEquals("yes") || exitCheck.contentEquals("y")) {
				exit = true;
			}
			System.out.println();

		}
		in.close();

	}

	public static String q1(String query) {

		// try with resources syntax to close connection and statement
		try (Connection myConn = DriverManager.getConnection(
				"jdbc:mysql://" + host + ":" + port + "/" + database + "?autoReconnect=true&useSSL=false", user,
				password); Statement statement = myConn.createStatement()) {

			// initialize the methods return string, the result from the query,
			// and metadata from the table
			StringBuilder qResult = new StringBuilder();
			ResultSet result = statement.executeQuery(query.toString());	//execute the user's query
			ResultSetMetaData metadata = result.getMetaData();
			int columnsNumber = metadata.getColumnCount();

			// -----formating the output string----
			// getting column names for first row of the output string
			for (int j = 1; j <= columnsNumber; j++) {
				qResult.append((metadata.getColumnName(j)));
				qResult.append("\t");
			}
			qResult.append("\n");
			
			// as long as there is another row add the row to the output
			while (result.next()) {
				for (int i = 1; i <= columnsNumber; i++) {	//for each column
					if (i > 1) {qResult.append(", \t");	}	//add comma before each column for every column except the first
					String columnValue = result.getString(i);	//add the column value
					qResult.append(columnValue + " ");			
				}
				qResult.append("\n");					//new line for the next row
			}
			
			
			// ------output check
			// if the returning string has only one line
			// it means that there was an empty set as respone
			String[] rows = qResult.toString().split("\\n");
			int countRows = 0;
			for (String lines : rows) {
				countRows++;
			}
			if (countRows == 1) {
				return "Empty Set"; // return empty set
			}

			return qResult.toString();

		} catch (Exception e) {
			// If there was an error
			return ("There was some error with your query: " + query.toString());
		}

	}

	private static String getStatementQ1() {
		
		boolean correctStatement = false;
		StringBuilder query = new StringBuilder();	//query to return
		
		
		// connect to database so we can get table and column data.
		// as always try with resource syntax with autoclosables.
		try (Connection myConn = DriverManager.getConnection(
				"jdbc:mysql://" + host + ":" + port + "/" + database + "?autoReconnect=true&useSSL=false", user,
				password); Statement statement = myConn.createStatement();) {
			
			System.out.println("Type \"e\" to exit the question at any moment.\n");
			Scanner input = new Scanner(System.in);
			StringBuilder tables = new StringBuilder();
			ResultSet showTables = statement.executeQuery("Show tables;");
			


			// logic behind getting all the iputs
			while (!correctStatement) {
				query.delete(0, query.length()); // if there was wrong inputs from a previous query
				
				while (showTables.next()) {
					tables.append(showTables.getString(1) + ",");
				}
				tables.delete(tables.length()-1, tables.length());	//deletes last ","
				
				//create an array with the table names to check if the input table is valid
				ArrayList<String> tableArray = new ArrayList<>();
				tableArray = new ArrayList<String>(Arrays.asList(tables.toString().split(",")));
				
				System.out.println("Current database's table(s)/view(s) are: ");
				System.out.println(tables.toString() + "\n");
				System.out.print("Type the table name and press enter: ");
				String table = input.nextLine();
				
				while (!tableArray.contains(table)) {
					if (table.toLowerCase().equals("e")) {
						return "";
					} else {
						System.out.println("Incorrect table/view name! The available tables/views are shown below: ");
						System.out.println(tables.toString() + "\n");
						System.out.print("Type the table name and press enter: ");
						table = input.nextLine();
						

					}
				}
				
				System.out.println();
				StringBuilder showColumns = new StringBuilder();
				try (Statement columnsStatement = myConn.createStatement();) {
					ResultSet columnSet = columnsStatement.executeQuery("Select * from " + table);
					ResultSetMetaData columMetadata = columnSet.getMetaData();
					int columnsNumber = columMetadata.getColumnCount();

					// -----formating the output string----
					// getting column names for first row
					for (int j = 1; j <= columnsNumber; j++) {
						showColumns.append((columMetadata.getColumnName(j)) + ",");
					}
					showColumns.delete(showColumns.length()-1, showColumns.length());	//deletes last ","
				}
				System.out.println(table + " table/view available column(s) are:");
				System.out.println(showColumns.toString() + "\n");
				System.out.print("Type the column name and press enter: ");
				String column = input.nextLine();
				
				ArrayList<String> columnArray = new ArrayList<>();
				columnArray = new ArrayList<String>(Arrays.asList(showColumns.toString().split(",")));
				
				while (!columnArray.contains(column)) {
					if (column.toLowerCase().equals("e")) {

						return "";
					}
					System.out.println();
					System.out.println("Incorrect column name! " + table + " table/view available column(s) are:");
					System.out.println(showColumns.toString());
					System.out.println();
					System.out.print("Please type one of the shown column(s) and press enter: ");
					column = input.nextLine();
					

				}

				System.out.print("\nType the value and press enter: ");
				String value = input.nextLine();
				;
				if (value.toLowerCase().equals("e")) {

					return "";
				}

				// a simple add-on
				System.out.print(
						"Type the kind of operator and press enter.The supported operators are ('=' , '>' , '<'): ");
				String operator = input.nextLine();
				;

				while (!(operator.equals("=") || operator.equals(">") || operator.equals("<"))) {
					if (operator.toLowerCase().equals("e")) {

						return "";
					}
					System.out.print("The operators you can use are ('=' , '>' , '<').Please type again: ");
					operator = input.nextLine();
					;
				}

				query.append("SELECT * ");
				query.append("FROM " + table);
				query.append(" WHERE " + column + " " + operator + " '" + value + "';");
				System.out.println();
				System.out.println("The Statement to be executed : " + query.toString());

				System.out.print("Is that the statement that you wanted? (y/n): ");
				String statementCheck;
				statementCheck = input.nextLine();
				;

				if (statementCheck.toLowerCase().equals("yes") || statementCheck.toLowerCase().equals("y")) {
					correctStatement = true;
				}

			}

			return query.toString();
		} catch (Exception e) {
			exit();
			return "";
			
		}

	}

	/**
	 * Connect to the given database Get all the course_id from course table For
	 * every course get every u_id For every u_id check whether it is in student or
	 * professor table
	 * 
	 */
	public static String q2() {
		StringBuilder finalResult = new StringBuilder(); // the return string to be build over the progression of the
															// method

		// -------establishing connection and first statement
		try (Connection myConn = DriverManager.getConnection(
				"jdbc:mysql://" + host + ":" + port + "/" + database + "?autoReconnect=true&useSSL=false", user,
				password); Statement statement = myConn.createStatement()) {

			// --getting all the course_id from the course table
			ResultSet courseIdQuery = statement.executeQuery("SELECT course_id FROM course");
			while (courseIdQuery.next()) {
				String courseId = courseIdQuery.getString(1); // NOTE: Hard coded! The Query must not change!

				finalResult.append("The course with id " + courseId + " is taught by:\t");

				// From this point on prepared statements is used for performance purposes.
				// getting all the teachers for the given course.
				try (java.sql.PreparedStatement teacherIdsQuery = myConn
						.prepareStatement("SELECT u_id FROM taughtby WHERE course_id = ?")) {

					teacherIdsQuery.setString(1, courseId); // NOTE: Hard coded! The Query must not change!
					ResultSet teacherIds = teacherIdsQuery.executeQuery();

					boolean hasTeacher = false;
					// for every teacherId
					while (teacherIds.next()) {
						// if it gets here it surely has teacher
						hasTeacher = true;
						finalResult.append(getTeacher(myConn, teacherIds)); // logic behind finding who is the teacher
					}
					if (!hasTeacher)
						finalResult.append("0");

				}
				finalResult.append("\n"); // add line to proceed for the next course id
			}

		} catch (Exception e) {
			exit();
			return "";
		}
		return finalResult.toString();

	}

	private static void exit() {
		System.out.println("The database connection cannot be established. Please read the README.txt");
		System.out.println("Present Project Directory : "+ System.getProperty("user.dir"));
		System.exit(0);
	}

	private static String getTeacher(Connection myConn, ResultSet teacherIds) throws SQLException {

		StringBuilder teacherPosDuo = new StringBuilder(); // result for this row
		String teacherId = teacherIds.getString(1); // current id

		// create two more prepare statements. they will check for the id in student and
		// professor table respectively
		try (java.sql.PreparedStatement isStudentQuery = myConn
				.prepareStatement("SELECT s_id FROM student WHERE s_id = ?");
				java.sql.PreparedStatement isProfessorQuery = myConn
						.prepareStatement("SELECT p_id FROM professor WHERE p_id = ?")) {

			// check if the teacher was student
			isStudentQuery.setString(1, teacherId);
			ResultSet studentResult = isStudentQuery.executeQuery();
			boolean wasStudent = false;

			while (studentResult.next()) {
				teacherPosDuo.append("(" + studentResult.getString(1) + ",student)   ");
				wasStudent = true; // if it has change the value and add the student to the result
			}

			// if it didn't have student it will surely have teacher
			// else it would be in this method
			if (!wasStudent) {
				isProfessorQuery.setString(1, teacherId);
				ResultSet professorResult = isProfessorQuery.executeQuery();

				while (professorResult.next()) {
					teacherPosDuo.append("(" + professorResult.getString(1) + ",professor)   ");
				}
			}

			return teacherPosDuo.toString();

		}

	}

}


//song related stuff..
class Song implements Runnable{
	Clip clip = null;
	AudioInputStream inputStream = null;
	public Song() {
		try{
			 clip = AudioSystem.getClip();
			 inputStream = AudioSystem.getAudioInputStream(new File(".\\resources\\gone with the wind.wav"));
			 clip.open(inputStream);
			 
			 
			 }catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void run() {
		if(clip.isActive()) {
			System.out.println("Song is already playing");
		}else {
			clip.start();
			clip.loop(Clip.LOOP_CONTINUOUSLY);}
			
			
			
			
			
	}
	
	public void stop() {
		if(!clip.isActive()) {
			System.out.println("Song is not currently playing");
		}else {
			clip.stop();
		}
		
	}
	

	
}


