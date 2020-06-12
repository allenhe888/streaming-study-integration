package com.bigdata.javastudy.advance;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Test;

public class MD5Demo {

    @Test
    public void testMD5(){
        String admin = DigestUtils.md5Hex("Dg&^AgkL");
        System.out.println(admin);
        byte[] md5 = DigestUtils.md5("Dg&^AgkL");
        System.out.println(md5);

    }

}
