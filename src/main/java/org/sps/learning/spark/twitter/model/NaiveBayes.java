package org.sps.learning.spark.twitter.model;

import org.sps.learning.spark.twitter.data.Document;
import org.sps.learning.spark.twitter.data.FeatureStats;
import org.sps.learning.spark.twitter.data.NaiveBayesKnowledgeBase;

import java.util.*;

public class NaiveBayes {

    private double chisquareCriticalValue = 10.83; //equivalent to pvalue 0.001. It is used by feature selection algorithm

    private NaiveBayesKnowledgeBase knowledgeBase;

    /**
     * This constructor is used when we load an already train classifier
     *
     * @param knowledgeBase
     */
    public NaiveBayes(NaiveBayesKnowledgeBase knowledgeBase) {
        this.knowledgeBase = knowledgeBase;
    }

    /**
     * train a new classifier.
     */
    public NaiveBayes() {
        this(null);
    }

    /**
     * Gets the knowledgebase parameter
     *
     * @return
     */
    public NaiveBayesKnowledgeBase getKnowledgeBase() {
        return knowledgeBase;
    }

    /**
     * Gets the chisquareCriticalValue paramter.
     *
     * @return
     */
    public double getChisquareCriticalValue() {
        return chisquareCriticalValue;
    }

    /**
     * Sets the chisquareCriticalValue parameter.
     *
     * @param chisquareCriticalValue
     */
    public void setChisquareCriticalValue(double chisquareCriticalValue) {
        this.chisquareCriticalValue = chisquareCriticalValue;
    }

    /**
     * Preprocesses the original dataset and converts it to a List of Documents.
     *
     * @param trainingDataset
     * @return
     */
    private List<Document> preprocessDataset(Map<String, String[]> trainingDataset) {
        List<Document> dataset = new ArrayList<>();

        String category;
        String[] examples;

        Document doc;

        //loop through all the categories and training examples
        for (Map.Entry<String, String[]> entry : trainingDataset.entrySet()) {
            category = entry.getKey();
            examples = entry.getValue();

            for (String example : examples) {
                //for each example in the category tokenize its text and convert it into a Document object.
                doc = TextTokenizer.tokenize(example);
                doc.category = category;
                dataset.add(doc);

                //examples[i] = null; //try freeing some memory
            }

            //it.remove(); //try freeing some memory
        }

        return dataset;
    }

    /**
     * Gathers the required counts for the features and performs feature selection
     * on the above counts. It returns a FeatureStats object that is later used
     * for calculating the probabilities of the model.
     *
     * @param dataset
     * @return
     */
    private FeatureStats selectFeatures(List<Document> dataset) {
        FeatureExtraction featureExtractor = new FeatureExtraction();

        //the FeatureStats object contains statistics about all the features found in the documents
        FeatureStats stats = featureExtractor.extractFeatureStats(dataset); //extract the stats of the dataset

        //we pass this information to the feature selection algorithm and we get a list with the selected features
        Map<String, Double> selectedFeatures = featureExtractor.chisquare(stats, chisquareCriticalValue);

        //clip from the stats all the features that are not selected
        Iterator<Map.Entry<String, Map<String, Integer>>> it = stats.featureCategoryJointCount.entrySet().iterator();
        while(it.hasNext()) {
            String feature = it.next().getKey();

            if(!selectedFeatures.containsKey(feature)) {
                //if the feature is not in the selectedFeatures list remove it
                it.remove();
            }
        }

        return stats;
    }

    /**
     * Trains a Naive Bayes classifier by using the Multinomial Model by passing
     * the trainingDataset and the prior probabilities.
     *
     * @param trainingDataset
     * @param categoryPriors
     * @throws IllegalArgumentException
     */
    public void train(Map<String, String[]> trainingDataset, Map<String, Double> categoryPriors) throws IllegalArgumentException {
        //preprocess the given dataset
        List<Document> dataset = preprocessDataset(trainingDataset);


        //produce the feature stats and select the best features
        FeatureStats featureStats =  selectFeatures(dataset);


        //intiliaze the knowledgeBase of the classifier
        knowledgeBase = new NaiveBayesKnowledgeBase();
        knowledgeBase.n = featureStats.n; //number of observations
        knowledgeBase.d = featureStats.featureCategoryJointCount.size(); //number of features


        //check is prior probabilities are given
        if(categoryPriors==null) {
            //if not estimate the priors from the sample
            knowledgeBase.c = featureStats.categoryCounts.size(); //number of cateogries
            knowledgeBase.logPriors = new HashMap<>();

            String category;
            int count;
            for(Map.Entry<String, Integer> entry : featureStats.categoryCounts.entrySet()) {
                category = entry.getKey();
                count = entry.getValue();

                knowledgeBase.logPriors.put(category, Math.log((double)count/knowledgeBase.n));
            }
        }
        else {
            //if they are provided then use the given priors
            knowledgeBase.c = categoryPriors.size();

            //make sure that the given priors are valid
            if(knowledgeBase.c!=featureStats.categoryCounts.size()) {
                throw new IllegalArgumentException("Invalid priors Array: Make sure you pass a prior probability for every supported category.");
            }

            String category;
            Double priorProbability;
            for(Map.Entry<String, Double> entry : categoryPriors.entrySet()) {
                category = entry.getKey();
                priorProbability = entry.getValue();
                if(priorProbability==null) {
                    throw new IllegalArgumentException("Invalid priors Array: Make sure you pass a prior probability for every supported category.");
                }
                else if(priorProbability<0 || priorProbability>1) {
                    throw new IllegalArgumentException("Invalid priors Array: Prior probabilities should be between 0 and 1.");
                }

                knowledgeBase.logPriors.put(category, Math.log(priorProbability));
            }
        }

        //We are performing laplace smoothing (also known as add-1). This requires to estimate the total feature occurrences in each category
        Map<String, Double> featureOccurrencesInCategory = new HashMap<>();

        Integer occurrences;
        Double featureOccSum;
        for(String category : knowledgeBase.logPriors.keySet()) {
            featureOccSum = 0.0;
            for(Map<String, Integer> categoryListOccurrences : featureStats.featureCategoryJointCount.values()) {
                occurrences=categoryListOccurrences.get(category);
                if(occurrences!=null) {
                    featureOccSum+=occurrences;
                }
            }
            featureOccurrencesInCategory.put(category, featureOccSum);
        }

        //estimate log likelihoods
        String feature;
        Integer count;
        Map<String, Integer> featureCategoryCounts;
        double logLikelihood;
        for(String category : knowledgeBase.logPriors.keySet()) {
            for(Map.Entry<String, Map<String, Integer>> entry : featureStats.featureCategoryJointCount.entrySet()) {
                feature = entry.getKey();
                featureCategoryCounts = entry.getValue();

                count = featureCategoryCounts.get(category);
                if(count==null) {
                    count = 0;
                }

                logLikelihood = Math.log((count+1.0)/(featureOccurrencesInCategory.get(category)+knowledgeBase.d));
                if(!knowledgeBase.logLikelihoods.containsKey(feature)) {
                    knowledgeBase.logLikelihoods.put(feature, new HashMap<>());
                }
                knowledgeBase.logLikelihoods.get(feature).put(category, logLikelihood);
            }
        }
    }

    /**
     * Wrapper method of train() which enables the estimation of the prior
     * probabilities based on the sample.
     *
     * @param trainingDataset
     */
    public void train(Map<String, String[]> trainingDataset) {
        train(trainingDataset, null);
    }

    /**
     * Predicts the category of a text by using an already trained classifier
     * and returns its score.
     *
     * if category is positive, returns a positive score
     * if category is negative, returns a negative score
     *
     * @param text
     * @return
     * @throws IllegalArgumentException
     */
    public Double predict(String text) throws IllegalArgumentException {
        if(knowledgeBase == null) {
            throw new IllegalArgumentException("Knowledge base missing. Training must be run first");
        }

        //Tokenizes the text and creates a new document
        Document doc = TextTokenizer.tokenize(text);


        String category;
        String feature;
        Integer occurrences;
        Double logprob;

        String maxScoreCategory = null;
        Double maxScore=Double.NEGATIVE_INFINITY;

        //Map<String, Double> predictionScores = new HashMap<>();
        for(Map.Entry<String, Double> entry1 : knowledgeBase.logPriors.entrySet()) {
            category = entry1.getKey();
            logprob = entry1.getValue(); //intialize the scores with the priors

            //foreach feature of the document
            for(Map.Entry<String, Integer> entry2 : doc.tokens.entrySet()) {
                feature = entry2.getKey();

                if(!knowledgeBase.logLikelihoods.containsKey(feature)) {
                    continue; //if the feature does not exist in the knowledge base skip it
                }

                occurrences = entry2.getValue(); //get its occurrences in text

                logprob += occurrences*knowledgeBase.logLikelihoods.get(feature).get(category); //multiply loglikelihood score with occurrences
            }
            //predictionScores.put(category, logprob);

            if(logprob>maxScore) {
                maxScore=logprob;
                maxScoreCategory=category;
            }
        }

        assert maxScoreCategory != null;
        if(maxScoreCategory.equals("Positive") && maxScore < 0)
            maxScore = maxScore * -1;  //changes maxScore to positive Double value if it is negative

        else if(maxScoreCategory.equals("Negative") && maxScore > 0)
            maxScore = maxScore * -1;   //changes maxScore to negative Double value if it is positive

        return maxScore; //returns a sentiment score
    }
}
