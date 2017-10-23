/* 
 * Copyright (C) 2014 Vasilis Vryniotis <bbriniotis at datumbox.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.sps.learning.spark.twitter.model;

import org.sps.learning.spark.twitter.data.Document;

import java.util.HashMap;
import java.util.Map;

/**
 * TextTokenizer class used to tokenize the texts and store them as Document
 * objects.
 * 
 * @author Vasilis Vryniotis <bbriniotis at datumbox.com>
 * @see <a href="http://blog.datumbox.com/developing-a-naive-bayes-text-classifier-in-java/">http://blog.datumbox.com/developing-a-naive-bayes-text-classifier-in-java/</a>
 */
public class TextTokenizer {
    
    /**
     * Preprocess the text by removing punctuation, duplicate spaces and 
     * lowercasing it.
     *
     * Search for negation and add an '!' before the following word.
     *
     * @param text
     * @return 
     */
    public static String preprocess(String text) {
        text = text.toLowerCase().replaceAll("\\p{P}", " ").replaceAll("\\s+", " ").replaceAll(" not ", " not !");
        text = text.replaceAll("no ", "no !");
        text = text.replaceAll("none ", "none !");
        text = text.replaceAll("nothing ", "nothing !");
        text = text.replaceAll("don't ", "don't !");
        text = text.replaceAll("never ", "never !");
        text = text.replaceAll("isn't ", "isn't !");
        text = text.replaceAll("didn't ", "didn't !");
        text = text.replaceAll("won't ", "won't !");
        text = text.replaceAll("hadn't ", "hadn't !");
        text = text.replaceAll("couldn't ", "couldn't !");
        return text; //.replaceAll("\\p{P}", " ").replaceAll("\\s+", " ");
    }
    
    /**
     * A simple method to extract the keywords from the text. For real world 
     * applications it is necessary to extract also keyword combinations.
     * 
     * @param text
     * @return 
     */
    public static String[] extractKeywords(String text) {
        return text.split(" ");
    }
    
    /**
     * Counts the number of occurrences of the keywords inside the text.
     * 
     * @param keywordArray
     * @return 
     */
    public static Map<String, Integer> getKeywordCounts(String[] keywordArray) {
        Map<String, Integer> counts = new HashMap<>();
        
        Integer counter;
        for (String aKeywordArray : keywordArray) {
            counter = counts.get(aKeywordArray);
            if (counter == null) {
                counter = 0;
            }
            counts.put(aKeywordArray, ++counter); //increase counter for the keyword
        }
        
        return counts;
    }
    
    /**
     * Tokenizes the document and returns a Document Object.
     * 
     * @param text
     * @return 
     */
    public static Document tokenize(String text) {
        String preprocessedText = preprocess(text);
        String[] keywordArray = extractKeywords(preprocessedText);
        
        Document doc = new Document();
        doc.tokens = getKeywordCounts(keywordArray);
        return doc;
    }
}
