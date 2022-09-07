package simpledb;

import org.junit.Test;
import simpledb.common.Type;

/**
 * @author bwdeng
 * @version 1.0.0
 * @ClassName HashCodeTest.java
 * @createTime 2022年09月07日 14:51:00
 */
public class HashCodeTest {
    @Test public void testHashCode(){
        Type  intType = Type.INT_TYPE;
        System.out.println(intType.hashCode() >> 4);
        Type  strType = Type.STRING_TYPE;
        System.out.println(strType.hashCode() >> 4);
    }
}
