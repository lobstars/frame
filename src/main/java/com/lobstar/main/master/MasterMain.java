package com.lobstar.main.master;

import java.io.IOException;
import java.util.UUID;

import com.lobstar.base.role.master.Master;
import com.lobstar.config.BuildConfiguration;

public class MasterMain {
    public static void main(String[] args) throws IOException {

        String name;
        String l = null;
        if (args.length < 1) {
            name = UUID.randomUUID().toString();
        } else {
            name = args[0];
        }
        if (args.length == 2) {
            name = args[0];
            l = args[1];
        }
        Master baseParkKeeper = null;
        try {
            baseParkKeeper = new Master(name, new BuildConfiguration().buildConfig());
            baseParkKeeper.initTicket(l, 10888);
            baseParkKeeper.tryBeKeeper();
        } catch (Exception e) {
            if (baseParkKeeper != null) {
                baseParkKeeper.close();
            }
        }
    }
}
