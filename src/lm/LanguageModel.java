package lm;

import javafx.util.Pair;
import mapreducejob.MapReduceJob;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class LanguageModel {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        switch (args[0]) {
            case "count":
                MapReduceJob.run(args[1], args[2]);
                break;
            case "calculate":
                System.out.println("P = " + calculate(args[1], args[2], args[3]));
                break;
            case "process":
                assert args.length == 3;
                Util.processData(args[1], args[2]);
                break;
            case "predict":
                assert args.length == 3;
                System.out.println(predict(args[1], args[2]));
                break;
            case "processCompat":
                if(args.length == 1){
                    Util.processDataCompat("/home/u18307130154/workplace/data/tripleGroup/rawdata", "/home/u18307130154/workplace/data/tripleGroup/");
                    Util.processDataCompat("/home/u18307130154/workplace/data/doubleGroup/rawdata", "/home/u18307130154/workplace/data/doubleGroup/");
                }
                else{
                    Util.processDataCompat(args[1], args[2]);
                }
                break;
            default:
                usage();
        }
    }

    private static void usage(){
        System.out.println("todo: usage");
    }

    private static String tripleGroupRecordPath = "/home/u18307130154/workplace/data/tripleGroup";
    private static String doubleGroupRecordPath = "/home/u18307130154/workplace/data/doubleGroup";

    private static String recordPath = "/home/u18307130154/workplace/data/doubleGroup";

    private static int getCount(WordGroup wordGroup) throws FileNotFoundException {
        int ind = Util.getSplitInd(wordGroup);
        Scanner scanner;
        if(wordGroup.size == 2){
            scanner = new Scanner(new FileReader(doubleGroupRecordPath + "/" + String.valueOf(ind + 1)));
        }
        else {
            scanner = new Scanner(new FileReader(tripleGroupRecordPath + "/" + String.valueOf(ind + 1)));
        }
        while(scanner.hasNextLine()){
            String line = scanner.nextLine();
            Pair<WordGroup, Integer> record = Util.parseCountRecord(line);
            if(wordGroup.equals(record.getKey())) return record.getValue();
        }
        return 0;
    }

    private static double calculate(String w1, String w2, String w3) throws FileNotFoundException {
//        double c1 = (double) getCount(new WordGroup(w1, w2));
//        double c2 = (double) getCount(new WordGroup(w1, w2, w3));
//        return (c2 + 1) / (c1 + 1);
        int ind = Util.getSplitInd(new WordGroup(w1, w2));
        Scanner scanner = new Scanner(new FileReader(recordPath + "/" + String.valueOf(ind + 1)));
        WordGroup target = new WordGroup(w1, w2);
        while(scanner.hasNextLine()){
            String line = scanner.nextLine();
            Pair<WordGroup, HashMap<String, Double>> record = Util.parseRecord(line);
            if(target.equals(record.getKey())) {
                HashMap<String, Double> probMap = record.getValue();
                if(probMap.containsKey(w3)){
                    return probMap.get(w3);
                }
                else{
                    return 0;
                }
            }
        }
        return 0;
    }

    private static String predict(String w1, String w2) throws FileNotFoundException {
        int ind = Util.getSplitInd(new WordGroup(w1, w2));
        Scanner scanner = new Scanner(new FileReader(recordPath + "/" + String.valueOf(ind + 1)));
        WordGroup target = new WordGroup(w1, w2);
        while(scanner.hasNextLine()){
            String line = scanner.nextLine();
            Pair<WordGroup, HashMap<String, Double>> record = Util.parseRecord(line);
            if(target.equals(record.getKey())) {
                HashMap<String, Double> probMap = record.getValue();
                double maxProb = 0;
                String maxWord = "$";
                for(Map.Entry<String, Double> entry : probMap.entrySet()){
                    if(entry.getValue() > maxProb){
                        maxProb = entry.getValue();
                        maxWord = entry.getKey();
                    }
                }
                return maxWord;
            }
        }
        return "$";
    }

}
