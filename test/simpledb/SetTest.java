package simpledb;

import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author bwdeng
 * @version 1.0.0
 * @ClassName SetTest.java
 * @createTime 2022年09月20日 14:35:00
 */
public class SetTest {

    Map<Integer, Set<Integer>> map;

    @Test
    public void testSet(){
        map = new HashMap<>();
        Set<Integer> set = map.getOrDefault(0, new HashSet<>());
        map.put(0,set);
        Set<Integer> set1 = map.get(0);
        set1.add(13);
        System.out.println(map.toString());
    }

    @Test

    public void testLock(){
        Lock lock = new ReentrantLock();
        lock.unlock();

    }
}
