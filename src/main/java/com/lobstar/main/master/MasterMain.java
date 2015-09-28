package com.lobstar.main.master;

import java.io.IOException;
import java.util.UUID;

import com.lobstar.base.role.master.Master;
import com.lobstar.config.Builder;

public class MasterMain {
    public static void main(String[] args) throws IOException {

        String name;
        if (args.length < 1) {
            name = UUID.randomUUID().toString();
        } else {
            name = args[0];
        }
        Master master = null;
        try {
            master = new Master(name);
            master.work();
        } catch (Exception e) {
        	e.printStackTrace();
            if (master != null) {
                master.close();
            }
        }
    }
}
