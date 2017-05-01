package org.apache.storm.wanglu.selfstudy.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by wanglu on 2017/5/1.
 */
public class Utils {

    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

    public static void waitForSeconds(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        }
        catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
        catch (Exception e){
            logger.error(e.getMessage(), e);
        }
    }

    public static void waitForMillis(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        }
        catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
        catch (Exception e){
            logger.error(e.getMessage(), e);
        }
    }
}
