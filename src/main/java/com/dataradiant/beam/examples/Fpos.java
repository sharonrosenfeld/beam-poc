package com.dataradiant.beam.examples;

import com.google.gson.JsonObject;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class Fpos {

    public static void main(String[] args) {

        try (BufferedWriter writer = new BufferedWriter(new FileWriter("/tmp/fpos.txt"))){
            Random rand = new Random();
            for (int i=0; i< 500000; ++i){
                //provider
                int p = rand.nextInt(10);
                String provider = String.format("%05d" , p);
                //channel
                int c = rand.nextInt(2);
                String channel = c == 0? "A" : "B";
                //105
                String errCode;
                if (p % 2 == 0){
                    errCode = "105";
                }else {
                    int b = rand.nextInt(6);
                    errCode = b > 2? "105" : "100";
                }
                JsonObject data = new JsonObject();
                data.addProperty("provider-id",provider);
                data.addProperty("channel-id",channel);
                data.addProperty("result",errCode);
                writer.write(data.toString()+"\n");
            }
        }catch (IOException exception){

        }
    }
}
