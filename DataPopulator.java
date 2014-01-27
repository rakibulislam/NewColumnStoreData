/*
 * DataPopulator.java
 * populate data from a file, each line is a json object
 * @author Jin Chen
 */

//package cheetah;

import java.io.FileReader;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import javax.json.Json;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.JsonObject;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonString;
import java.nio.ByteBuffer;


public class DataPopulator {
    public static void main(String[] args){

        String inputFileName; 
        int buffer_size = 2*1000*1000;
        System.out.println("Buffer size (inGB) "+buffer_size/1000/1000/1000); 
        //Argo1 argo1Store = new Argo1(buffer_size);
        ColStore dataStore = new ColStore(buffer_size); 

        if (args.length != 1){
            System.out.println("Arguments: "+ "<input file name>" );
            System.exit(0);
        }

        try{
            inputFileName = args[0];
            BufferedReader bufferedReader = new BufferedReader(new FileReader(inputFileName));
            String line = bufferedReader.readLine();
            int objid = 0;
            while (line !=null){
                objid  = objid + 1;
                InputStream is = new ByteArrayInputStream(line.getBytes());
                JsonReader reader = Json.createReader(is);
                JsonObject jsonob = reader.readObject();
                //System.out.println(jsonob.toString());
                //dataStore.insertRow(objid,jsonob,null);
                dataStore.insertObject(objid,jsonob,null);
                line = bufferedReader.readLine();
                if(objid > 300)
                    break; //avoid overflow - for testing purpose
                
            }

            /* output the row - for test*/
            dataStore.getObject(3);

        } catch (FileNotFoundException e){
            System.err.println("FileNotFoundException:"+e.getMessage());
            return ;
        } catch (IOException e){
            System.err.println("IOException:"+e.getMessage());
            return ;
        }
    }
}

