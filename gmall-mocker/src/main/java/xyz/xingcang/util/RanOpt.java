package xyz.xingcang.util;

/**
 * @author xingcang
 * @create 2020-11-03 6:23 PM
 */
public class RanOpt<T>{
    private T value ;
    private int weight;

    public RanOpt ( T value, int weight ){
        this.value=value ;
        this.weight=weight;
    }

    public T getValue() {
        return value;
    }

    public int getWeight() {
        return weight;
    }
}
