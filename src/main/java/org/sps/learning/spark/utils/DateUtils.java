package org.sps.learning.spark.utils;

import org.apache.log4j.Logger;
import org.sps.learning.spark.algorithms.ml.kmeans.Featurization;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

/**
 * This code is licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * ==Overview==
 * <p>
 * This class
 * <p>
 * Created by ypriverol (ypriverol@gmail.com) on 25/10/2017.
 */
public class DateUtils {

    private static final Logger LOGGER = Logger.getLogger(DateUtils.class);

    private static long beginTime = Timestamp.valueOf("2009-04-01 00:00:00").getTime();
    private static long endTime   = Timestamp.valueOf("2013-05-31 23:58:00").getTime();

    /**
     * Method should generate random number that represents
     * a time between two dates.
     *
     * @return
     */
    private static long getRandomTimeBetweenTwoDates () {
        long diff = endTime - beginTime + 1;
        return beginTime + (long) (Math.random() * diff);
    }

    public static String generateRandomDateJava() {

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-hhmmss");
        Date randomDate = new Date(getRandomTimeBetweenTwoDates());
        return dateFormat.format(randomDate);

    }
}
