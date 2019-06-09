package org.uma.jmetalsp.externalsources;

import org.apache.commons.io.FileUtils;
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
 * Execution:
 *
 *      cd path/to/jMetalSP
 *      java -cp jmetalsp-externalsource/target/jmetalsp-externalsource-2.1-SNAPSHOT-jar-with-dependencies.jar \
 *          org.uma.jmetalsp.externalsources.TrafficSource outputTraffic 60 csv
 *
 * @author Marcos Hernández Marcelino
 */

public class TrafficSource {

    private void startCsv(String outputDirectory, long frequency) {
        System.out.println("TrafficSource::startCsv::Init parameters " + outputDirectory + " " + Long.toString(frequency));

        String sURL = "https://data.cityofnewyork.us/resource/i4gi-tjb9.csv";
        int counter = 0 ;
        boolean keepRunning = true;

        while (keepRunning) {
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
                System.out.println("TrafficSource::startCsv::Got new data...");

                if(errorCode == 200) {
                    // Get response
                    BufferedReader requestResponse = new BufferedReader(
                            new InputStreamReader(connection.getInputStream()));

                    // Write to a buffer
                    String line;
                    StringBuffer lineBuffer = new StringBuffer();
                    while ((line = requestResponse.readLine()) != null) {
                        lineBuffer.append(line);
                        lineBuffer.append('\n');
                    }

                    // Close StringBuffer
                    requestResponse.close();

                    // Write to file
                    FileOutputContext fileOutputContext = new DefaultFileOutputContext(outputDirectory + "/" + counter + "-traffic") ;
                    BufferedWriter bufferedWriter = fileOutputContext.getFileWriter() ;
                    bufferedWriter.write(lineBuffer.toString());
                    bufferedWriter.close();

                    counter++;

                    // Check if new file is duplicated
                    //if(!deleteDuplicated(counter, outputDirectory)) {
                    //    counter++;
                    //}
                } else {
                    System.out.println("TrafficSource::startCsv::Wrong HTTP response");
                }
            } catch (MalformedURLException e) {
                System.out.println(e.toString());
            } catch (IOException e) {
                System.out.println(e.toString());
            }

            try {
                Thread.sleep(frequency * 1000);
            } catch (InterruptedException e) {
                keepRunning = false;
                System.out.println("TrafficSource::startCsv::Thread sleep interruption");
            }
        }
    }

    private void startJson(String outputDirectory, long frequency) {
        System.out.println("TrafficSource::startJson::Init parameters " + outputDirectory + " " + Long.toString(frequency));

        String sURL = "https://data.cityofnewyork.us/resource/i4gi-tjb9.json";
        int counter = 0 ;
        boolean keepRunning = true;

        while (keepRunning) {
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
                System.out.println("TrafficSource::startJson::Got new data...");

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
                    FileOutputContext fileOutputContext = new DefaultFileOutputContext(outputDirectory + "/" + counter + "-traffic") ;
                    BufferedWriter bufferedWriter = fileOutputContext.getFileWriter() ;
                    for (Object jObject: jData) {
                        // Get JSON
                        JSONObject jLine = (JSONObject) jObject;

                        // Store parsed line
                        bufferedWriter.write(jLine.toString());
                        bufferedWriter.write('\n');
                    }

                    bufferedWriter.close();

                    // Check if new file is duplicated
                    if(!deleteDuplicated(counter, outputDirectory)) {
                        counter++;
                    }
                } else {
                    System.out.println("TrafficSource::startJson::Wrong HTTP response");
                }
            } catch (MalformedURLException e) {
                System.out.println(e.toString());
            } catch (IOException e) {
                System.out.println(e.toString());
            } catch (ParseException e) {
                System.out.println(e.toString());
            }

            try {
                Thread.sleep(frequency * 1000);
            } catch (InterruptedException e) {
                keepRunning = false;
                System.out.println("TrafficSource::startJson::Thread sleep interruption");
            }
        }
    }

    private boolean deleteDuplicated(int index, String path) {
        // Not the first file
        if(index > 0) {
            // Store current and previous files
            File previousFile = new File(path + "/" + Integer.toString(index-1) + "-traffic");
            File currentFile = new File(path + "/" + Integer.toString(index) + "-traffic");

            try {
                // Compare if their contents are equals
                if(FileUtils.contentEquals(currentFile, previousFile)) {
                    System.out.println("TrafficSource::deleteDuplicated::Same file then remove current file");
                    // Remove duplicated
                    FileUtils.forceDelete(currentFile);
                    return true;
                } else {
                    System.out.println("TrafficSource::deleteDuplicated::Different files then store");
                    return false;
                }
            } catch (IOException e) {
                System.out.println("TrafficSource::deleteDuplicated::IOException: " + e.getMessage());
                return false;
            }
        } else {
            System.out.println("TrafficSource::deleteDuplicated::First file");
            return false;
        }
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

    public static void main(String[] args) throws Exception {
        System.out.println("TrafficSource::Main");

        if(args.length < 3) {
            throw new Exception("Invalid number of arguments. Output directory, frequency and format needed.");
        }

        // Create output directory
        String directory = args[0];
        createDataDirectory(directory);
        // Get thread frequency
        long frequency = Long.parseLong(args[1]);

        // Creates an object and call start method
        if(args[2].equals("json")) {
            new TrafficSource().startJson(directory, frequency);
        } else {
            new TrafficSource().startCsv(directory, frequency);
        }
    }

}
