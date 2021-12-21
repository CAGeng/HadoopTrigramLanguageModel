package lm;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WordGroup {

    public int size;

    public ArrayList<String> words;

    public String groupStr;

    public WordGroup(String w1, String w2, String w3){
        words = new ArrayList<>(Arrays.asList(w1, w2, w3));
        size = 3;
        groupStr = w1 + " " + w2 + " " + w3;
     }

    public WordGroup(String w1, String w2){
        words = new ArrayList<>(Arrays.asList(w1, w2));
        size = 2;
        groupStr = w1 + " " + w2;
    }

    public WordGroup(List<String> wordList){

        assert wordList.size() == 3 || wordList.size() == 2;

        if(wordList.size() == 3){
            words = new ArrayList<>(wordList);
            size = 3;
            groupStr = wordList.get(0) + " " + wordList.get(1) + " " + wordList.get(2);
        }
        else {
            words = new ArrayList<>(wordList);
            size = 2;
            groupStr = wordList.get(0) + " " + wordList.get(1);
        }
    }

    public String toString(){
        return groupStr;
    }

    public int hash(){
        String hashStr = words.get(0) + " " + words.get(1);
        return hashStr.hashCode() & Integer.MAX_VALUE;
    }

    public boolean equals(WordGroup wg){
        return wg.groupStr.equals(this.groupStr);
    }
}
