
/**
 * ResultSet
 *
 * @author Alan Lu
 */
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.File;
import javax.json.Json;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.JsonObject;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonString;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Hashtable;
import java.util.HashMap;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

public class ResultSetV2{
    protected long UNDEFINED= -1111111; //represent NULL
    
    /* special character to seprate key and type, use : for now*/
    protected String separator=":";
    
    public long[] longValues;
    public int index;
    public int[] oids;
    public int[] indexes;
    public long[] longValuesTemp;
    public int[] oidsTemp;
    
    public String[] keys;
    public String[] keysTemp;
    public int largestObjectId;
    public boolean shuffle;
    public int[] map;

    public ResultSetV2(int size){
        longValues = new long[size];
        oids = new int[size];
        keys = new String[size];
        longValuesTemp = new long[size];
        oidsTemp = new int[size];
        keysTemp = new String[size];
        map = new int[largestObjectId+1];
        indexes = new int[size];
        
        index = 0;
        this.largestObjectId = 300000;
        shuffle = false;
    }
    
    public ResultSetV2(int size, int largestObjectId){
        longValues = new long[size];
        oids = new int[size];
        keys = new String[size];
        longValuesTemp = new long[size];
        oidsTemp = new int[size];
        keysTemp = new String[size];
        map = new int[largestObjectId+1];
        indexes = new int[size];
        
        index = 0;
        this.largestObjectId = largestObjectId;
        shuffle = false;
    }
    
    public void addLong(String key, int oid, long longnum){
        longValuesTemp[index] = longnum;
        keysTemp[index] = key;
        oidsTemp[index] = oid;
        
        if(shuffle == false){
            if(index > 0){
                if(oid < oidsTemp[index-1]){
                    shuffle = true;
                }
            }
        }
        
        indexes[(oid*40 + map[oid])] = index;
        map[oid]++;

        index++;
    }
    
    
    public void shuffle(){
        
        long start = System.currentTimeMillis();
        
        if(shuffle){
            int count = 0;
            for(int i=0; i<map.length; i++){
                for(int j=0; j<map[i];j++){
                    longValues[count] = longValuesTemp[indexes[i*40+j]];
                    oids[count] = oidsTemp[indexes[i*40+j]];
                    keys[count] = keysTemp[indexes[i*40+j]];
                    count++;
                }
            }
        }/*else{
            System.arraycopy( oidsTemp, 0, oids, 0, index );
            System.arraycopy( keysTemp, 0, keys, 0, index );
            System.arraycopy( longValuesTemp, 0, longValues, 0, index );
        }*/
        
        long end = System.currentTimeMillis();
        
        System.out.print("(" + (end-start) + ")");
    }
    
    public void clearResultSet(){
        map = new int[largestObjectId+1];
        index = 0;
        //largestObjectId = 0;
        shuffle = false;
    }
    
    //return all selected objects in a HashMap
    public HashMap<Integer, String> selectObjects(){
        //TODO:
   		     return null;
    }
    
    public HashMap<Integer, HashMap<String, Long>> getAllResultsInHashMap(){
        HashMap<Integer, HashMap<String, Long>> resultSet = new HashMap<Integer, HashMap<String, Long>>();
        for(int i = 0; i < index; i++){
            int oid;
            long value;
            String key;
            
            if(shuffle){
                oid = oids[i];
                key = keys[i];
                value = longValues[i];
            }
            else{
                oid = oidsTemp[i];
                key = keysTemp[i];
                value = longValuesTemp[i];
            }
            
            
            HashMap<String, Long> inner = resultSet.get(oid);
            if(inner == null){
                inner = new HashMap<String, Long>();
            }
            inner.put(key, value);
            resultSet.put(oid, inner);
        }
     
        return resultSet;
    }
    
    public void printBriefStats(){
        HashMap<Integer, String> resultSet = new HashMap<Integer, String>();
        for(int i = 0; i < index; i++){
            int oid;
            if(shuffle)
                oid = oids[i];
            else
                oid = oidsTemp[i];
                
            resultSet.put(oid, "");
        }
        System.out.print("selected " + resultSet.size() + " objects with " + index + " fields");
        return;
    }
}
