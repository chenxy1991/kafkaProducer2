package com.thread2.Consumer;

import com.thread2.Utils.Utils;
import junit.framework.TestCase;

public class UtilsTest extends TestCase {

    public void testGetKey() {
        System.out.println(Utils.getKey("1524707442686_cluster=teledb,udal,instance=132.121.197.149:8022,host=db1m9.db.ng,proj=cbs,job=cbs,m=idle"));
    }
}