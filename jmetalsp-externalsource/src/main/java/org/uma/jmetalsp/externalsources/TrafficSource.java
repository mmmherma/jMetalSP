package org.uma.jmetalsp.externalsources;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.uma.jmetal.util.fileoutput.FileOutputContext;
import org.uma.jmetal.util.fileoutput.impl.DefaultFileOutputContext;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * This class reads New York city "real time" traffic data using a JSON based API. Based on CounterProvider class.
 *
 * @author Marcos Hernández Marcelino
 */

public class TrafficSource {

    private void start(String outputDirectory) {
        System.out.println("start::Init");

        String sURL = "https://data.cityofnewyork.us/resource/i4gi-tjb9.json";
        int counter = 0 ;

        while (true) {
            try {
                // Set URL
                URL url = new URL(sURL);
                // Open connection
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();

                // Set GET as method
                connection.setRequestMethod("GET");

                // Set request type
                connection.setRequestProperty("Content-Type", "application/json");

                // Get response type
                int errorCode = connection.getResponseCode();
                System.out.println("start::Got new data...");

                if(errorCode == 200) {
                    // Get response
                    BufferedReader requestResponse = new BufferedReader(
                            new InputStreamReader(connection.getInputStream()));

                    // Write to a buffer
                    String line;
                    StringBuffer lineBuffer = new StringBuffer();
                    while ((line = requestResponse.readLine()) != null) {
                        lineBuffer.append(line);
                    }

                    // Close StringBuffer
                    requestResponse.close();

                    // Convert to JSON
                    JSONParser jParser = new JSONParser();
                    JSONArray jData = (JSONArray) jParser.parse(lineBuffer.toString());

                    // Write to file
                    FileOutputContext fileOutputContext = new DefaultFileOutputContext(outputDirectory + "/time." + counter) ;
                    BufferedWriter bufferedWriter = fileOutputContext.getFileWriter() ;
                    for (Object jObject: jData) {
                        JSONObject j = (JSONObject) jObject;

                        bufferedWriter.write(j.toString());
                        bufferedWriter.write('\n');
                    }

                    bufferedWriter.close();

                    counter++;
                } else {
                    System.out.println("start::Wrong HTTP response");
                }
            } catch (MalformedURLException e) {
                System.out.println(e.toString());
            } catch (IOException e) {
                System.out.println(e.toString());
            } catch (ParseException e) {
                System.out.println(e.toString());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("TrafficSource::Main");

        if(args.length != 1) {
            throw new Exception("Invalid number of arguments. Output directory needed.");
        }

        // Create output directory
        String directory = args[0];
        createDataDirectory(directory);

        // Creates an object and call start method
        new TrafficSource().start(directory);
    }

    private static void createDataDirectory(String outputDirectoryName) {
        File outputDirectory = new File(outputDirectoryName);

        if (outputDirectory.isDirectory()) {
            System.out.println("createDataDirectory::The output directory exists. Deleting and creating ...");
            for (File file : outputDirectory.listFiles()) {
                file.delete();
            }
            outputDirectory.delete();
        } else {
            System.out.println("createDataDirectory::The output directory doesn't exist. Creating ...");
        }

        new File(outputDirectoryName).mkdir();
    }

}
