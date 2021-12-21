package test;


import com.alibaba.fastjson.JSON;
import lm.Util;
import lm.WordGroup;
import org.apache.hadoop.util.hash.Hash;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.StringTokenizer;

public class Test {
    public static void main(String[] args) throws IOException {
//        testParseCount();
//        testList();

//        String line = "a b {\"c\":0.2,\"d\":0.3}";
//        ArrayList<String> lineList = Util.splitString(line);
//        assert lineList.size() == 3;
//        WordGroup wordGroup = new WordGroup(lineList.subList(0, 2));
//        String probText = lineList.get(2);
//        HashMap<String, Integer> probMap = JSON.parseObject(probText, HashMap.class);
//        for(Object w3 : probMap.keySet()){
//            System.out.println(w3);
//        }

//        testParseProbText();
        Util.parseRecord("aaa both        {from=0.2, relapsing=0.2, traumas=0.2, espectrito=0.2, he=0.2}");
    }

    public static void test1(){
        String line = "i am a sb";
        String[] wordList = line.split(" ");
        for(int i = 0; i < wordList.length - 2; i++){
            System.out.println(wordList[i] + wordList[i + 1] + wordList[i + 2]);
        }

        File file = new File("/out", "test");
        System.out.println(file.getPath());
    }

    public static void testParseCount() throws IOException {
        lm.Util.processDataCompat("C:\\Users\\sft\\Desktop\\code\\HadoopTriGramLM\\data\\count", "C:\\Users\\sft\\Desktop\\code\\HadoopTriGramLM\\data");
    }

    public static void testList() {
        ArrayList<String> arrayList = new ArrayList<>(Arrays.asList("1", "2", "3"));
        WordGroup wordGroup = new WordGroup(arrayList.subList(0,2));
        System.out.println(wordGroup);
    }

    public static void testParseProbText(){
        HashMap<String, Double> hashMap = Util.parseProvText("{a=1, b=2, c=3}");
        for(String key : hashMap.keySet()){
            System.out.println(key);
            System.out.println(hashMap.get(key));
        }
    }
}
