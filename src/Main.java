import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.*;
import java.sql.*;
import java.util.Properties;

public class Main {

    public static void main(String[] args) {
        try {
            Connection connection = getConnection();
            String query = getQueryFromInputFile();
            System.out.println("Executing query : "+query);
            JSONObject jsonObjectByExecutingQuery = getJsonObjectByExecutingQuery(query);

            Gson gson = new GsonBuilder().setPrettyPrinting().create();

            try (FileWriter file = new FileWriter("output.json")) {
                String jsonOutput = gson.toJson(jsonObjectByExecutingQuery);
                file.write(jsonOutput);
                file.flush();

            } catch (IOException e) {
                e.printStackTrace();
            }

            System.out.print("Output.json generated");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static JSONObject getJsonObjectByExecutingQuery(String query){
        JSONObject allRowsObject = new JSONObject();
        JSONArray list = new JSONArray();
        Connection connection = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            connection = getConnection();
            stmt = connection.createStatement();
            rs = stmt.executeQuery( query );
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            Boolean nextPatientFound = true;
            String patientId = "";
            JSONObject patientJsonObject = new JSONObject();
            JSONArray eventList = new JSONArray();


            //Get patient data from First Row
            rs.next();
            patientJsonObject.put("birthDate", rs.getString("birth_date"));
            patientJsonObject.put("gender", rs.getString("gender"));
            patientJsonObject.put("events",eventList);
            patientId = rs.getString("patient_id");
            nextPatientFound=false;
            //Moving cursor one step back
            rs.previous();

            while ( rs.next() ) {

                //determine if patientID has changed
                //if true, then new patient row has been found.
                if(rs.getString("patient_id").equals(patientId) == false){
                    patientId = rs.getString("patient_id");
                    nextPatientFound = true;
                }

                //nextPatientFound == false means still getting
                //same patient ID in each row
                if(nextPatientFound == false){
                    JSONObject eventJsonObject = new JSONObject();
                    eventJsonObject.put("date",rs.getString("date"));
                    eventJsonObject.put("code",rs.getString("code"));
                    eventList.add(eventJsonObject);

                } else {
                    list.add(patientJsonObject);

                    //New Entry
                    patientJsonObject = new JSONObject();
                    eventList = new JSONArray();
                    patientJsonObject.put("birthDate", rs.getString("birth_date"));
                    patientJsonObject.put("gender", rs.getString("gender"));
                    patientJsonObject.put("events",eventList);

                    //reset boolean
                    nextPatientFound = false;
                }
            }



        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            try{
                if (stmt != null){
                    stmt.close();
                }
                if(rs!=null){
                    rs.close();
                }
                if(connection!=null){
                    connection.close();
                }
            }catch (SQLException e1){
                System.out.println(e1.getMessage());
            }
        }

        allRowsObject.put("rows",list);
        return allRowsObject;
    }

    public static String getQueryFromInputFile() throws IOException {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader("input.txt"));
            String line = br.readLine();
            return line;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        finally {
            br.close();
        }
        return null;
    }

    public static Connection getConnection() throws SQLException {

        Properties properties = new Properties();
        InputStream in = null;

        try {
            File file = new File("dbConnect.properties");
            in = new FileInputStream(file);
            properties.load(in);
        } catch(IOException ioe) {
            ioe.printStackTrace();
        }

        String drivers = properties.getProperty("jdbc.drivers");
        String url = properties.getProperty("jdbc.url");
        String username = properties.getProperty("jdbc.username");
        String password = properties.getProperty("jdbc.password");

        return DriverManager.getConnection(url, username, password);

    }
}
