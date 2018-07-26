import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.oracle.javafx.jmx.json.JSONException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.*;
import java.sql.*;
import java.text.ParseException;
import java.util.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

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

            //loop until result set is not finished.
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
                    //Get events list here and sort them
                    JSONArray events = (JSONArray)patientJsonObject.get("events");
                    sortEventsList(events);

                    //here we are adding min,max,median for one patient object
                    //based on events
                    patientJsonObject.put("maxTimeline",getMaxFromEvents(events));
                    patientJsonObject.put("minTimeline",getMinFromEvents(events));
                    patientJsonObject.put("medianTimeline",getMedianFromEvents(events));


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

    private static double getMedianFromEvents(JSONArray events) {
        if(events.size() > 1){
            final String KEY_NAME = "date";
            List<Long> deltasList = new ArrayList<>();

            for(int i=0;i<events.size()-1;i++){
                JSONObject firstObject = (JSONObject) events.get(i);
                JSONObject secondObject = (JSONObject) events.get(i+1);

                Date firstObjectDate = null;
                Date secondObjectDate = null;

                String valA = (String) firstObject.get(KEY_NAME);
                String valB = (String) secondObject.get(KEY_NAME);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                try {
                    firstObjectDate = sdf.parse(valA);
                    secondObjectDate = sdf.parse(valB);

                    long diff = secondObjectDate.getTime() - firstObjectDate.getTime();
                    long daysDiff = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
                    deltasList.add(daysDiff);
                } catch (ParseException e) {
                    e.printStackTrace();
                }

            }//end-for-loop

            OptionalDouble average = deltasList
                    .stream()
                    .mapToDouble(a -> a)
                    .average();

            return average.isPresent() ? average.getAsDouble() : 0;
        }

        return -1;
    }

    private static long getMaxFromEvents(JSONArray events) {
        if(events.size() > 1){
            final String KEY_NAME = "date";
            int totalLength = events.size();
            JSONObject firstObject = (JSONObject) events.get(0);
            JSONObject lastObject = (JSONObject) events.get(totalLength - 1);

            Date firstObjectDate = null;
            Date lastObjectDate = null;

            String valA = (String) firstObject.get(KEY_NAME);
            String valB = (String) lastObject.get(KEY_NAME);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            try {
                firstObjectDate = sdf.parse(valA);
                lastObjectDate = sdf.parse(valB);

                long diff = lastObjectDate.getTime() - firstObjectDate.getTime();
                return TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
            } catch (ParseException e) {
                e.printStackTrace();
            }

            return -1;
        }

        return -1;
    }

    private static long getMinFromEvents(JSONArray events) {
        if(events.size() > 1){
            final String KEY_NAME = "date";
            long minDays = Long.MAX_VALUE;

            for(int i=0;i<events.size()-1;i++){
                JSONObject firstObject = (JSONObject) events.get(i);
                JSONObject secondObject = (JSONObject) events.get(i+1);

                Date firstObjectDate = null;
                Date secondObjectDate = null;

                String valA = (String) firstObject.get(KEY_NAME);
                String valB = (String) secondObject.get(KEY_NAME);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                try {
                    firstObjectDate = sdf.parse(valA);
                    secondObjectDate = sdf.parse(valB);

                    long diff = secondObjectDate.getTime() - firstObjectDate.getTime();
                    long days = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
                    if(days < minDays){
                        minDays = days;
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }

            }//end-for-loop

            return minDays;
        }

        return -1;
    }

    private static void sortEventsList(JSONArray events) {

        Collections.sort( events, new Comparator<JSONObject>() {
            //You can change "Name" with "ID" if you want to sort by ID
            private static final String KEY_NAME = "date";

            @Override
            public int compare(JSONObject a, JSONObject b) {
                String valA = new String();
                String valB = new String();
                Date date1 = null;
                Date date2 = null;
                try {
                    valA = (String) a.get(KEY_NAME);
                    valB = (String) b.get(KEY_NAME);
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                    date1 = sdf.parse(valA);
                    date2 = sdf.parse(valB);
                }
                catch (Exception e) {
                    e.printStackTrace(System.err);
                }

                return date1.compareTo(date2);
                //if you want to change the sort order, simply use the following:
                //return -valA.compareTo(valB);
            }
        });

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
