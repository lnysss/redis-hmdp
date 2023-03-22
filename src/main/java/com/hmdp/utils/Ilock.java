package com.hmdp.utils;

public interface Ilock {

    boolean trylock(long timeSec);

    void setlock();

}
