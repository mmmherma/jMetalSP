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
 *          org.uma.jmetalsp.externalsources.TrafficSource outputTraffic 60
 *
 * @author Marcos Hernández Marcelino
 */

public class TrafficSource {

    private void start(String outputDirectory, long frequency) {
        System.out.println("TrafficSource::start::Init parameters " + outputDirectory + " " + Long.toString(frequency));

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
                System.out.println("TrafficSource::start::Got new data...");

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
                    boolean first = true;
                    for (Object jObject: jData) {
                        // Get JSON
                        JSONObject jLine = (JSONObject) jObject;

                        if(first) {
                            bufferedWriter.write(getHeader(jLine) + '\n');
                            first = false;
                        }

                        // Store parsed line. From JSON to CSV
                        bufferedWriter.write(parseJsonToCsv(jLine));
                        bufferedWriter.write('\n');
                    }

                    bufferedWriter.close();

                    // Not the first file
                    if(counter > 0) {
                        // Store current and previous files
                        File previousFile = new File(outputDirectory + "/" + Integer.toString(counter-1) + "-traffic");
                        File currentFile = new File(outputDirectory + "/" + Integer.toString(counter) + "-traffic");

                        // Compare if their contents are equals
                        if(FileUtils.contentEquals(currentFile, previousFile)) {
                            System.out.println("TrafficSource::start::Same file then remove current file");
                            // Remove duplicated
                            FileUtils.forceDelete(currentFile);
                        } else {
                            System.out.println("TrafficSource::start::Different files then store");
                            counter++;
                        }
                    } else {
                        System.out.println("TrafficSource::start::First file");
                        counter++;
                    }
                } else {
                    System.out.println("TrafficSource::start::Wrong HTTP response");
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
                System.out.println("TrafficSource::start::Thread sleep interruption");
            }
        }
    }

    private String parseJsonToCsv(JSONObject line) {
        // Parsed line
        String parsedLine = "";

        // Iterate over JSON line and create CSV line
        for(Object key : line.keySet()) {
            parsedLine += "\"" + line.get(key).toString() + "\"" + ",";
        }

        // Return parsed line
        return parsedLine.substring(0, parsedLine.length()-1);
    }

    private String getHeader(JSONObject line) {
        // Parsed line
        String headerLine = "";

        // Iterate over JSON line and create CSV line
        for(Object key : line.keySet()) {
            headerLine += "\"" + key.toString() + "\"" + ",";
        }

        // Return parsed line
        return headerLine.substring(0, headerLine.length()-1);
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

        if(args.length < 2) {
            throw new Exception("Invalid number of arguments. Output directory and frequency needed.");
        }

        // Create output directory
        String directory = args[0];
        createDataDirectory(directory);
        // Get thread frequency
        long frequency = Long.parseLong(args[1]);

        // Creates an object and call start method
        new TrafficSource().start(directory, frequency);
    }

}
