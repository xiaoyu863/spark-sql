package com.qf.util;

import java.util.Arrays;

public class GetMyInfo {

    public static String getMyInfo(String website,int which) {
        if(which==0) return getSubject(website);
        else return getModel(website);
    }

    public static String getSubject(String info){
        int start = info.indexOf("//");
        int end = info.indexOf(".");
        if(info.length()!=0) {
            String str = info.substring(start + 2, end);
            return str;
        }
        return "/";
    }

    public static String getModel(String info) {
        int start = info.lastIndexOf("/");
        int end = info.lastIndexOf(".");
        if(info.length()!=0) {
            String str = info.substring(start + 1, end);
            return str;
        }
        return "/";
    }

}
