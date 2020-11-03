package xyz.xingcang.util;

import java.util.Random;

/**
 * @author xingcang
 * @create 2020-11-03 6:26 PM
 */
public class RandomNum {
    public static int getRandInt(int fromNum,int toNum){
        return fromNum + new Random().nextInt(toNum-fromNum+1);
    }
}
