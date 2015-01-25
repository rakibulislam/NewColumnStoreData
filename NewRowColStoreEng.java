
/**
 * NewRowColStore
 *
 * Assumption: we don't have synchronization so far   
 *             so no need to protect about buffer array acess 
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

public class NewRowColStoreEng extends StoreEngine {
    
	private ByteBuffer stringBuffer;
    //Hashtable<String, Long> tempBufLong;
	//Hashtable<String, Double> tempBufDouble;
	//Hashtable<String, String> tempBufString;
	//Hashtable<String, String> tempBufBool;
    
	Hashtable<String, partitionV2> partitions;
    Hashtable<String, String> searchPar;
    ResultSetV2 result;
    
	public NewRowColStoreEng (int memory_size_in_bytes,String layoutFile)
	{
		super(memory_size_in_bytes);
        stringBuffer = ByteBuffer.allocateDirect(max_buf_size/10);
        stringBuffer.position(0);
        
        //tempBufLong = new Hashtable<String, Long>();
        //tempBufDouble = new Hashtable<String, Double>();
        //tempBufString = new Hashtable<String, String>();
		//tempBufBool = new Hashtable<String, String>();
        
        partitions = new Hashtable<String, partitionV2>();
        searchPar = new Hashtable<String, String>();
        result = new ResultSetV2(100*1000*1000);
        
        /* read layout file, create partition for each line */
        /* key is field_name:type, value is the buf */
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(layoutFile));
            String line  = bufferedReader.readLine();
            //int i = 0;
            while(line !=null){
                String[] keys = line.split("\\s+"); //separted by any white space
                for (String k : keys)
                    searchPar.put(k, line);
                
                if(keys.length == 0)
                    continue; // empty line

                int size;
				if((keys.length==1)&&(line.contains("sparse_"))){
                    size = max_buf_size/8/1000*(keys.length)/100;
				}else{
                    size = max_buf_size/8/1000*(keys.length);
				}
                partitionV2 par = new partitionV2(line, size);
                partitions.put(line, par);
                
                line = bufferedReader.readLine();
            }
        } catch (FileNotFoundException e){
			System.err.println("FileNotFoundException:"+e.getMessage());
			return ;
        } catch (IOException e){
			System.err.println("IOException:"+e.getMessage());
			return;
        }
	}
    
    public void insertObject(int objid, JsonValue tree, String key){
		//tempBufLong.clear();
		//tempBufDouble.clear();
		//tempBufString.clear();
		//tempBufBool.clear();
		
        prepareRow(objid, tree, key);
		//insertRow(objid);
        
        for(String par: partitions.keySet()){
            partitions.get(par).doneInsertingObject(objid);
        }
	}
    
    public void prepareRow(int objid, JsonValue tree, String key){
		switch(tree.getValueType()){
			case OBJECT:
				JsonObject object = (JsonObject) tree;
				for(String name: object.keySet()){
					if(key!=null)
						prepareRow(objid,object.get(name),key+"."+name);
					else
						prepareRow(objid,object.get(name),name);
				}
				break;
			case ARRAY:
				JsonArray array = (JsonArray) tree;
				int index =0;
				for (JsonValue val : array){
					prepareRow(objid,val,key+"["+index+"]");
					index += 1;
				}
				break;
			case STRING:
				JsonString st = (JsonString) tree;
				//tempBufString.put(key+separator+"STRING", st.getString());
                if(searchPar.get(key+separator+"STRING") != null){
                    partitions.get(searchPar.get(key+separator+"STRING")).insertObject(objid, key+separator+"STRING", stringBuffer.position());
                    
                    stringBuffer.putInt(st.getString().length());
                    stringBuffer.put(st.getString().getBytes());
                }
				break;
			case NUMBER:
				JsonNumber num = (JsonNumber) tree;
                if(num.isIntegral()){
					//tempBufLong.put(key+separator+"LONG", num.longValue());
                    //System.out.println(key+separator+"LONG" + num.longValue());
                    if(searchPar.get(key+separator+"LONG") != null){
                        partitions.get(searchPar.get(key+separator+"LONG")).insertObject(objid, key+separator+"LONG", num.longValue());
                    }
                    

                }else{
					//tempBufDouble.put(key+separator+"DOUBLE", num.doubleValue());
                    if(searchPar.get(key+separator+"DOUBLE") != null){
                        partitions.get(searchPar.get(key+separator+"DOUBLE")).insertObject(objid, key+separator+"DOUBLE", (long)num.doubleValue() * 100000);
                    }
                }
				break;
			case TRUE:
                if(searchPar.get(key+separator+"BOOL") != null)
                    partitions.get(searchPar.get(key+separator+"BOOL")).insertObject(objid, key+separator+"BOOL", (long)1);
                break;
			case FALSE:
                if(searchPar.get(key+separator+"BOOL") != null)
                    partitions.get(searchPar.get(key+separator+"BOOL")).insertObject(objid, key+separator+"BOOL", (long)0);
				//tempBufBool.put(key+separator+"BOOL", tree.getValueType().toString());
				break;
			case NULL:
				break;
		}
	}
    
    /*
	public void prepareRow(int objid, JsonValue tree, String key){
		switch(tree.getValueType()){
			case OBJECT:
				JsonObject object = (JsonObject) tree;
				for(String name: object.keySet()){
					if(key!=null)
						prepareRow(objid,object.get(name),key+"."+name);
					else
						prepareRow(objid,object.get(name),name);
				}
				break;
			case ARRAY:
				JsonArray array = (JsonArray) tree;
				int index =0;
				for (JsonValue val : array){
					prepareRow(objid,val,key+"["+index+"]");
					index += 1;
				}
				break;
			case STRING:
				JsonString st = (JsonString) tree;
				tempBufString.put(key+separator+"STRING", st.getString());
				break;
			case NUMBER:
				JsonNumber num = (JsonNumber) tree;
                if(num.isIntegral()){
					tempBufLong.put(key+separator+"LONG", num.longValue());
                }else{
					tempBufDouble.put(key+separator+"DOUBLE", num.doubleValue());
                }
				break;
			case TRUE:
			case FALSE:
				tempBufBool.put(key+separator+"BOOL", tree.getValueType().toString());
				break;
			case NULL:
				break;
		}
	}
    
	public void insertRow(int objid){
        for(String par: partitions.keySet()){
            partitions.get(par).insertObject(objid, tempBufLong, tempBufDouble, tempBufString, tempBufBool, stringBuffer);
        }
    }*/
    
    //TODO: finish this
	public int getObject(int targetId)
	{
        for(String par: partitions.keySet()){
            partitions.get(par).getObjectFromPartition(targetId);
        }
        return 0;
    }
    
    public void select(byte[][] columns){
        result.clearResultSet();

        for(String par: partitions.keySet()){
            partitions.get(par).select(columns, result);
        }
        return;
    }
    
    /* select x,y,z,... where a between value1 and value2
     * range query, single column, long  type -- need to extend its type to include double
     */
    public void selectRange(byte[][] selectCols, byte[] whereCol, long value1, long value2){
        //clear the result data structure that contains selected data in memory
        result.clearResultSet();

        String where = (new String(whereCol)) + separator + "LONG";
        List<Integer> oidList = partitions.get( searchPar.get(where) ).selectRangeWherePar(selectCols, whereCol, value1, value2, result);
        
        if(oidList.size() > 0){
            Hashtable<String, String> parsToSelect = new Hashtable<String, String>();
            
            if(selectCols[0][0]==(byte) '*' ){//select all fields
                for(String par: partitions.keySet()){
                    parsToSelect.put(par, "");
                }
            }else{
                for(byte[] select: selectCols){
                    String selectField = new String(select);
                    if(searchPar.get(selectField + separator + "LONG") != null)
                        parsToSelect.put(searchPar.get(selectField + separator + "LONG"), "");
                    
                    if(searchPar.get(selectField + separator + "DOUBLE") != null)
                        parsToSelect.put(searchPar.get(selectField + separator + "DOUBLE"), "");
                    
                    if(searchPar.get(selectField + separator + "STRING") != null)
                        parsToSelect.put(searchPar.get(selectField + separator + "STRING"), "");
                    
                    if(searchPar.get(selectField + separator + "BOOL") != null)
                        parsToSelect.put(searchPar.get(selectField + separator + "BOOL"), "");
                }
            }
            //the partition that contains the WHERE has been already selected as we checked condition
            parsToSelect.remove( searchPar.get(where) );
            
            //convert List to array of oids
            int[] oids = new int[oidList.size()];
            for(int i = 0; i < oidList.size(); i++)
                oids[i] = oidList.get(i);
            
            for(String par: parsToSelect.keySet())
                partitions.get(par).selectCondition(oids, selectCols, result);
                //partitions.get(par).selectCondition(oidList, selectCols, result);
        }
        
        result.shuffle();

        return;
    }
    
    /*
     * select where value = ANY xxx
     *  xxx is a set / JASON array, need to compare each member of this set
     */
	public HashMap<Integer, HashMap<String, String>> selectWhereAny(byte[][] selectCols, byte[] whereCol, String relation, byte[] value){
        //clear the result data structure that contains selected data in memory
        result.clearResultSet();
        
        Hashtable<String, String> wherePars = new Hashtable<String, String>();
        List<Integer> oidList = new ArrayList<Integer>();
        
        // scan each related column xx[0],xx[1],...
        int count = 0;
        while(true){
            String whereKey = new String(whereCol)+"["+String.valueOf(count)+"]"+separator+"STRING";
            if(searchPar.get(whereKey) == null){
                break;
            }
            count++;
            wherePars.put(searchPar.get(whereKey),"");
        }
        
        for(String par: wherePars.keySet()){
            oidList = partitions.get( par ).selectWhereAnyCheckCondition(whereCol, relation, value, stringBuffer, oidList);
        }
        
        Collections.sort(oidList);
        
        if(oidList.size() > 0){
            Hashtable<String, String> parsToSelect = new Hashtable<String, String>();
            
            if(selectCols[0][0]==(byte) '*' ){//select all fields
                for(String par: partitions.keySet()){
                    parsToSelect.put(par, "");
                }
            }else{
                for(byte[] select: selectCols){
                    String selectField = new String(select);
                    if(searchPar.get(selectField + separator + "LONG") != null)
                        parsToSelect.put(searchPar.get(selectField + separator + "LONG"), "");
                    
                    if(searchPar.get(selectField + separator + "DOUBLE") != null)
                        parsToSelect.put(searchPar.get(selectField + separator + "DOUBLE"), "");
                    
                    if(searchPar.get(selectField + separator + "STRING") != null)
                        parsToSelect.put(searchPar.get(selectField + separator + "STRING"), "");
                    
                    if(searchPar.get(selectField + separator + "BOOL") != null)
                        parsToSelect.put(searchPar.get(selectField + separator + "BOOL"), "");
                }
            }
            //convert List to array of oids
            int[] oids = new int[oidList.size()];
            for(int i = 0; i < oidList.size(); i++)
                oids[i] = oidList.get(i);
            
            for(String par: parsToSelect.keySet())
                partitions.get(par).selectCondition(oids, selectCols, result);
            //partitions.get(par).selectCondition(oidList, selectCols, result);
        }
        return null;
	}
    
    /*
     * select x,y,z where a = xx or a < xx or a > xx
     * single column and single relation parsing
     * Method: scan the where column and find the oid which meets the condition
     *               for each selected oid, get the values from select columns
     */
    public HashMap<Integer, HashMap<String, String>> selectWhereSingle(byte[][] selectCols, byte[] whereCol, String relation, byte[] value){
        //clear the result data structure that contains selected data in memory
        result.clearResultSet();
        
        String where = (new String(whereCol)) + separator + "STRING";
        List<Integer> oidList = partitions.get( searchPar.get(where) ).selectWhereSingleWherePar(selectCols, whereCol, relation, value, result, stringBuffer);
        
        if(oidList.size() > 0){
            Hashtable<String, String> parsToSelect = new Hashtable<String, String>();
            
            if(selectCols[0][0]==(byte) '*' ){//select all fields
                for(String par: partitions.keySet()){
                    parsToSelect.put(par, "");
                }
            }else{
                for(byte[] select: selectCols){
                    String selectField = new String(select);
                    if(searchPar.get(selectField + separator + "LONG") != null)
                        parsToSelect.put(searchPar.get(selectField + separator + "LONG"), "");
                    
                    if(searchPar.get(selectField + separator + "DOUBLE") != null)
                        parsToSelect.put(searchPar.get(selectField + separator + "DOUBLE"), "");
                    
                    if(searchPar.get(selectField + separator + "STRING") != null)
                        parsToSelect.put(searchPar.get(selectField + separator + "STRING"), "");
                    
                    if(searchPar.get(selectField + separator + "BOOL") != null)
                        parsToSelect.put(searchPar.get(selectField + separator + "BOOL"), "");
                }
            }
            //the partition that contains the WHERE has been already selected as we checked condition
            parsToSelect.remove( searchPar.get(where) );
            
            //convert List to array of oids
            int[] oids = new int[oidList.size()];
            for(int i = 0; i < oidList.size(); i++)
                oids[i] = oidList.get(i);
            
            for(String par: parsToSelect.keySet())
                partitions.get(par).selectCondition(oids, selectCols, result);
            //partitions.get(par).selectCondition(oidList, selectCols, result);
        }
        return null;
    }
    
    /* 
     * select count(*) where a between value1 and value2 GROUP By K
     * Note: seems selectCols are not needed
     */
    public Hashtable<String, Integer> aggregateRangeGroupBy(byte[][] selectCols, byte[] whereCol, long value1, long value2, byte[] gColumn)
    {
        //result.clearResultSet();

        //First check condition and get a list of oids
        String where = (new String(whereCol)) + separator + "LONG";
        List<Integer> oidList = partitions.get( searchPar.get(where) ).aggregateRangeGroupByCheckCondition(whereCol, value1, value2);
        Hashtable<String, Integer> resultSet = new Hashtable<String, Integer>();
        //Then do the count based on the gColumn
        if(oidList.size() > 0){
            //convert List to array of oids
            int[] oids = new int[oidList.size()];
            for(int i = 0; i < oidList.size(); i++)
                oids[i] = oidList.get(i);

            String gField = new String(gColumn);
            if(searchPar.get(gField + separator + "LONG") != null)
                resultSet = partitions.get(searchPar.get(gField + separator + "LONG")).aggregateGroupBy(gColumn, oids, "LONG", stringBuffer);

            if(searchPar.get(gField + separator + "STRING") != null)
                resultSet = partitions.get(searchPar.get(gField + separator + "STRING")).aggregateGroupBy(gColumn, oids, "STRING", stringBuffer);
            
            if(searchPar.get(gField + separator + "DOUBLE") != null)
                resultSet = partitions.get(searchPar.get(gField + separator + "DOUBLE")).aggregateGroupBy(gColumn, oids, "DOUBLE", stringBuffer);
            
            if(searchPar.get(gField + separator + "BOOL") != null)
                resultSet = partitions.get(searchPar.get(gField + separator + "BOOL")).aggregateGroupBy(gColumn, oids, "BOOL", stringBuffer);
        }
		return resultSet;
    }
    /*
    public HashMap<String, Integer> joinLeft(byte[] jCol, byte[] whereCol, long value1, long value2)
    {
        //scan this table, evaluate the condition, and build a hashtable
        String where = (new String(whereCol)) + separator + "LONG";
        List<Integer> oidList = partitions.get( searchPar.get(where) ).evaluateRangeCondition(whereCol, value1, value2);
        HashMap<String, Integer> hash = new HashMap<String, Integer>();

        if(oidList.size() > 0){
            //build the hashtable: key - jCol values, value: objid
            //convert List to array of oids
            int[] oids = new int[oidList.size()];
            for(int i = 0; i < oidList.size(); i++)
                oids[i] = oidList.get(i);
            
            String jField = new String(jCol);
            if(searchPar.get(jField + separator + "LONG") != null)
                partitions.get(searchPar.get(jField + separator + "LONG")).scanLeft(oids, jCol, hash, "LONG");
            
            if(searchPar.get(selectField + separator + "DOUBLE") != null)
                partitions.get(searchPar.get(jField + separator + "DOUBLE")).scanLeft(oids, jCol, hash, "DOUBLE");
            
            if(searchPar.get(selectField + separator + "STRING") != null)
                partitions.get(searchPar.get(jField + separator + "STRING")).scanLeft(oids, jCol, hash, "STRING");
            
            if(searchPar.get(selectField + separator + "BOOL") != null)
                partitions.get(searchPar.get(jField + separator + "STRING")).scanLeft(oids, jCol, hash, "BOOL");
            
        }
        
        return hash;
    }
    
    public HashMap<String, Integer> joinRight(byte[] jCol, HashMap<String, Integer> hash){
        
    }
    
    public void selectAfterJoin(){
        
    }
*/
    
    //print the total number of null values in the table
    public int getNullCount(){
        int count = 0;
        for(String par: partitions.keySet())
            count += partitions.get(par).getNullCount();

        return count;
    }
    
    public void printBriefStats(){
        result.printBriefStats();
        return;
    }
    
    public void printLayoutInfo(){
        int max_partition_size = 0;
        int num_partitions = 0;
        for(String par: partitions.keySet()){
            String[] keys = par.split("\\s+"); //separted by any white space
            if(keys.length > max_partition_size)
                max_partition_size = keys.length;
            num_partitions++;
        }
        //System.out.print("num partitions: " + num_partitions + ", max partition size: " + max_partition_size);
        System.out.print(max_partition_size + "_" + num_partitions);

        return;
    }
    
    public void printResultsToFile(String Query, String outputfile){
        HashMap<Integer, HashMap<String, Long>> resultSet = result.getAllResultsInHashMap();
        ByteBuffer readBuf = stringBuffer.asReadOnlyBuffer();
        
        try{
            String filename= outputfile;
            FileWriter fw = new FileWriter(filename,true); //the true will append the new data
            //first output the query
            fw.write(Query + ":\n");
            
            for(Integer objid: resultSet.keySet()){
                HashMap <String, Long> inner = resultSet.get(objid);
                
                fw.write("object " + objid + " {");
                
                for(String key: inner.keySet()){
                    String[] parts = key.split(separator);
                    fw.write("\"" + parts[0] + "\": ");
                    
                    if(parts[1].equals("STRING")){
                        long pos = inner.get(key).longValue();
                        readBuf.position((int)pos);
                        int len = readBuf.getInt();
                        byte [] valstr = new byte[len];
                        readBuf.get(valstr);
                        fw.write("\"" + new String(valstr) + "\", ");
                    }else if(parts[1].equals("LONG")){
                        fw.write(inner.get(key).longValue() + ", ");
                    }else if(parts[1].equals("DOUBLE")){
                        double num = (double) ( inner.get(key).longValue() / 100000);
                        fw.write(num + ", ");
                    }else if(parts[1].equals("BOOL")){
                        long bool = inner.get(key).longValue();
                        if(bool == 1){
                            fw.write("true, ");
                        }else{
                            fw.write("false, ");
                        }
                    }
                }
                fw.write("}\n");
            }
            fw.close();
        }
        catch(IOException ioe){
            System.err.println("IOException: " + ioe.getMessage());
        }
    }

	public static void main(String[] args) throws IOException{
		/* flatten the json file */ 
		JsonReader reader = Json.createReader(new FileReader("testjson/abcde3.json"));
		JsonObject jsonob = reader.readObject();
		System.out.println(jsonob.toString());
		NewRowColStoreEng parser= new NewRowColStoreEng(100*1000*1000,"testjson/abcde3.layout");
        
        for(int objid = 0; objid < 100000; objid++){
            parser.insertObject(objid,jsonob,null);
        }
        
        byte[] where = "A".getBytes();
        
        byte[][] columns = new byte[4][];
        columns[0] = "A".getBytes();
        columns[1] = "B".getBytes();
        columns[2] = "P".getBytes();
        columns[3] = "Q".getBytes();
        
        for(int i = 0; i < 10; i++){
            long start = System.currentTimeMillis();
            parser.select(columns);
            //HashMap<Integer, HashMap<String, String>> resultSet = parser.selectRange(columns,where,0,100);
            long end = System.currentTimeMillis();
            System.out.print((end-start) + " ");
        }
        System.out.println();
        parser.printBriefStats();
	}
}
