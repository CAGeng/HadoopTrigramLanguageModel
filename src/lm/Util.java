package lm;

import com.alibaba.fastjson.JSON;
import javafx.util.Pair;
import org.apache.hadoop.util.hash.Hash;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Scanner;

public class Util {

    public static int splitNum = 2000;

    public static int getSplitInd(WordGroup wordGroup){
        return wordGroup.hash() % splitNum;
    }

    public static void processData(String rawDataPath, String outputPath) throws IOException {
        Scanner scanner = new Scanner(new FileReader(rawDataPath));
        ArrayList<FileWriter> fileWriters = initFileWriters(outputPath);
        while (scanner.hasNextLine()){
            String line = scanner.nextLine();
            try{
                Pair<WordGroup, HashMap<String, Double>> recordPair = parseRecord(line);
                fileWriters.get(getSplitInd(recordPair.getKey())).write(line + "\n");
            }catch (Exception e){
                e.printStackTrace();
                System.out.println(line);
                break;
            }
        }
        for(FileWriter fileWriter : fileWriters) fileWriter.close();
    }

    public static ArrayList<String> splitString(String line){
        String[] lineList = formatWhiteSpace(line).split(" ", 3);
        return new ArrayList<>(Arrays.asList(lineList));
    }

    public static Pair<WordGroup, Integer> parseCountRecord(String line){
        ArrayList<String> lineList = splitString(line);
        WordGroup wordGroup = new WordGroup(lineList.subList(0, lineList.size() - 1));
        return new Pair<>(wordGroup, Integer.parseInt(lineList.get(lineList.size() - 1)));
    }

    public static Pair<WordGroup, HashMap<String, Double> > parseRecord(String line){
        ArrayList<String> lineList = splitString(line);
        assert lineList.size() == 3;
        WordGroup wordGroup = new WordGroup(lineList.subList(0, 2));
        String probText = lineList.get(2);
        HashMap<String, Double> probMap = parseProvText(probText);
        return new Pair<>(wordGroup, probMap);
    }

    public static String formatWhiteSpace(String raw){
        return raw.replaceAll("\\s+", " ").trim();
    }

    public static HashMap<String, Double> parseProvText(String probText){
        String[] splits = formatWhiteSpace(probText.substring(1, probText.length() - 1).replaceAll(",", " ")).split(" ");
        HashMap<String, Double> probMap = new HashMap<>();
        for(String unit : splits){
            String[] rec = unit.split("=");
            assert rec.length == 2;
            probMap.put(rec[0], Double.valueOf(rec[1]));
        }
        return probMap;
    }

    private static ArrayList<FileWriter> initFileWriters(String outputPath) throws IOException {
        ArrayList<FileWriter> fileWriters = new ArrayList<>();
        for(int i = 0; i < splitNum; i++){
            FileWriter fileWriter = new FileWriter(new File(outputPath, String.valueOf(i + 1)));
            fileWriters.add(fileWriter);
        }
        return fileWriters;
    }

    /**
     * @deprecated
     */
    public static void processDataCompat(String rawDataPath, String outputPath) throws IOException {
        Scanner scanner = new Scanner(new FileReader(rawDataPath));
        ArrayList<FileWriter> fileWriters = initFileWriters(outputPath);
        while (scanner.hasNextLine()){
            String line = scanner.nextLine();
            try{
                Pair<WordGroup, Integer> recordPair = parseCountRecord(line);
                fileWriters.get(getSplitInd(recordPair.getKey())).write(line + "\n");
            }catch (Exception e){
                e.printStackTrace();
                System.out.println(line);
                break;
            }
        }
        for(FileWriter fileWriter : fileWriters) fileWriter.close();
    }


}
