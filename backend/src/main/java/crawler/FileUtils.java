package crawler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONObject;

/**
 * Utility class for parsing input files
 * @author Daniel
 *
 */
public class FileUtils {
    /**
     * Parses the given file and returns the lines as a list of strings
     * @param filePath
     * @return
     */
    public static List<String> getFileLines(String filePath) {
        try {
            if (filePath == null) {
                return new ArrayList<String>();
            }
            return Files.readAllLines(Paths.get(filePath));
        } catch (IOException e) {
            return new ArrayList<String>();
        }
    }
    
    /**
     * Parses the given file as a JSON object
     * @param configPath
     * @return
     */
    public static JSONObject readConfig(String configPath) {
        try {
            List<String> lines = Files.readAllLines(Paths.get(configPath));
            String fileContents = String.join("", lines);
            return new JSONObject(fileContents);
        }
        catch (IOException e) {
            return new JSONObject();
        }
    }
}
