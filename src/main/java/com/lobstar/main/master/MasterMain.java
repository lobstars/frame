package com.lobstar.main.master;

import java.io.IOException;
import java.util.UUID;

import com.lobstar.base.role.master.Master;
import com.lobstar.config.BuildConfiguration;
import com.lobstar.config.Builder;

public class MasterMain {
    public static void main(String[] args) throws IOException {

        String name;
        if (args.length < 1) {
            name = UUID.randomUUID().toString();
        } else {
            name = args[0];
        }
        Master baseParkKeeper = null;
        try {
            baseParkKeeper = new Master(name, new Builder().buildConfig(),false);
            baseParkKeeper.tryBeKeeper();
        } catch (Exception e) {
        	e.printStackTrace();
            if (baseParkKeeper != null) {
                baseParkKeeper.close();
            }
        }
    }
}
