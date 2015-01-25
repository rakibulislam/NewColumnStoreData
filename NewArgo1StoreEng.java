
/**
 * An row buffer store using Argo1 representation 
 * @author Jin Chen
 */
import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import javax.json.Json;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.JsonObject;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonString;
import java.nio.ByteBuffer;
import java.util.Hashtable;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;


public class NewArgo1StoreEng extends StoreEngine  {

	//private int max_buf_size;
	private ByteBuffer buffer; //save all of the contents
	private ByteBuffer stringBuffer;
    ResultSetV2 result;
	//private int  UNDEFINED= -1111111; //represent NULL
    //private byte [] UndefByte= new byte[1];

	/**
     * Creates a ring buffer, and using Argo1 format to store the data
	 * initialize the buffer size 
     */
    public NewArgo1StoreEng (int memory_size_in_bytes)
    {
		//int max_buf_size = memory_size_in_bytes; 
		
		//call parent constructor
		super(memory_size_in_bytes);
		// create a byte buffer 
		buffer = ByteBuffer.allocateDirect(max_buf_size);
		stringBuffer = ByteBuffer.allocateDirect(max_buf_size);
		result = new ResultSetV2(100*1000*1000);
		//buffer = new byte[max_buf_size];
       // UndefByte[0] = -1;
	}

	/**
	 * [objid,keystr,valstr,valnum,valbool]
     * objid - int, 4Bytes
     * keystr = int + chars, 4Bytes + length
     * valstr = 8 byte storing position to stringBuffer
     * valnum = 8 byte storing long number
	 * valbool = 8 byte
	 */

	protected int saveStrValue(int objid, String key, String value)
	{
		buffer.putInt(objid);
		buffer.putInt(key.length());
		buffer.put(key.getBytes());
        
        long position = stringBuffer.position();
        stringBuffer.putInt(value.length());
        stringBuffer.put(value.getBytes());

		//buffer.putInt(value.length());
		//buffer.put(value.getBytes());
        buffer.putLong(position);
        
		buffer.putLong(UNDEFINED);
		//buffer.put(UndefByte);
        buffer.putLong(UNDEFINED);
        
		return 0;
	}

	protected int saveLongValue(int objid, String key, long num)
	{
		buffer.putInt(objid);
		buffer.putInt(key.length());
		buffer.put(key.getBytes());
		//buffer.putInt(UNDEFINED);
        buffer.putLong(UNDEFINED);
		buffer.putLong(num);
		//buffer.put(UndefByte);
        buffer.putLong(UNDEFINED);
        
		return 0;
	} 

	protected int saveDoubleValue(int objid, String key, double num)
	{
		buffer.putInt(objid);
		buffer.putInt(key.length());
		buffer.put(key.getBytes());
		//buffer.putInt(UNDEFINED);
        buffer.putLong(UNDEFINED);
		//buffer.putDouble(num);
        buffer.putLong((long)num*100000);
		//buffer.put(UndefByte);
        buffer.putLong(UNDEFINED);
		return 0;
	} 

	protected int saveBoolValue(int objid, String key, String value)
	{
		buffer.putInt(objid);
		buffer.putInt(key.length());
		buffer.put(key.getBytes());
		//buffer.putInt(UNDEFINED);
        buffer.putLong(UNDEFINED);
		buffer.putLong(UNDEFINED);
		if(value.equals("TRUE")==true){
			//buffer.put((byte)1);
            buffer.putLong(1);
		}else if(value.equals("FALSE")==true){
			//buffer.put((byte)0);
            buffer.putLong(0);
		}else{
			System.out.println("Error: unknow value "+value);
		}
		return 0;
	}


	/**
	* Navigate the json object and parse it and save it into storage 
	*
	*/

	public void insertRow(int objid, JsonValue tree, String key){
        //leave 1K as the warning threshold - don't insert after it 
        if(buffer.position() > buffer.capacity() - 1000){
            //don't insert any more -- buffer almost full
            System.out.println("buffer is almost full. No inserts any more!");
            return;
        }
		switch(tree.getValueType()){
			case OBJECT:
				//System.out.println("  OBJECT");
				JsonObject object = (JsonObject) tree;
				for(String name: object.keySet()){
					if(key!=null)
						insertRow(objid,object.get(name),key+"."+name);
					else
						insertRow(objid,object.get(name),name);
				}
                // check the size
                //if((objid % 10000) == 1) 
                //   System.out.println("Row id " + objid+ "buffer offset "+buffer.position());
				break;
			case ARRAY:
				//System.out.println("  ARRAY");
				JsonArray array = (JsonArray) tree;
				int index =0; 
				for (JsonValue val : array){
					insertRow(objid,val,key+"["+index+"]");
					index += 1;
				}
				break;
			case STRING:
				JsonString st = (JsonString) tree;
				saveStrValue(objid,key,st.getString());
				//System.out.println(objid+" "+key+" "+st.getString());
				break;
			case NUMBER:
				JsonNumber num = (JsonNumber) tree;
                if(num.isIntegral()){
				    saveLongValue(objid,key,num.longValue());
                }else{
                    saveDoubleValue(objid,key,num.doubleValue());
                }
				//System.out.println(objid+" "+key+" "+num.toString());
				break;
			case TRUE:
			case FALSE:
				saveBoolValue(objid,key,tree.getValueType().toString());
				//System.out.println(objid+" "+key+" "+tree.getValueType().toString());
				break;
			case NULL:
                // we didn't save null value
				//System.out.println("null\n:");
				//System.out.println(objid+"key "+key+tree.getValueType().toString());
				break;
		}
	}
    public void select(byte[][] columns){
        result.clearResultSet();
        
        ByteBuffer readBuf = buffer.asReadOnlyBuffer();
        int bound = buffer.position(), len, oid = 1;
        // start from the beginning
		readBuf.position(0);
		byte [] key,valstr, valbool = new byte[1];
		long valnum=0,boolnum=0,pos=0;
        
        while(readBuf.position()<bound){
			oid = readBuf.getInt();
            //read the field key
            len = readBuf.getInt();
            key = new byte [len];
            readBuf.get(key);
            if(isSelected(columns, key) == true){ //should select this field
                pos = readBuf.getLong();
                valnum = readBuf.getLong();
                boolnum = readBuf.getLong();
                
                if(pos == UNDEFINED){ //this is not a string
                    if(valnum == UNDEFINED){ // this is a bool
                        result.addLong(new String(key), oid, boolnum);
                    }else{ // this is a long
                        result.addLong(new String(key), oid, valnum);
                    }
                }else{
                    result.addLong(new String(key), oid, pos);
                }
            }else
                readBuf.position(readBuf.position() + 8 + 8 + 8);
        }
        
        return;
    }
    
    /* select x,y,z,... where a between value1 and value2
     * range query, single column, long  type -- need to extend its type to include double
     */
    public void selectRange(byte[][] selectCols, byte[] whereCol, long value1, long value2)
    {
        result.clearResultSet();
        
        ByteBuffer readBuf = buffer.asReadOnlyBuffer();
        
        int bound = buffer.position(), len, preOid = -1, oid = 1, startPos = 0;
        // start from the beginning
		readBuf.position(0);
        
		byte [] key,valstr, valbool = new byte[1];
		long valnum=0,boolnum=0,pos=0;
		int conditionFlag = 0; //0:not checked, 1:checked and selected, 2: checked and not selected
		while(readBuf.position()<bound){
			oid = readBuf.getInt();
			if(oid != preOid){
				//new object starting
				preOid = oid;
				startPos = readBuf.position() - 4;
				conditionFlag = 0;
			}
            
            //pos = readBuf.getLong();
            //stringReadBuf.position(pos);
            //len = stringReadBuf.getInt();
            len = readBuf.getInt();
            
            if(conditionFlag == 0){
                //read the key
                key = new byte [len];
                //stringReadBuf.get(key);
                readBuf.get(key);
                
                //if this is the WHERE field
                if(Arrays.equals(key, whereCol)==true){
                    readBuf.position(readBuf.position() + 8);
                    valnum = readBuf.getLong();

                    conditionFlag = 2;
                    if( (valnum >= value1) && (valnum <= value2)){
                        conditionFlag = 1;
                        //if the condition is met, position back to to the start of the object
                        readBuf.position(startPos);
                    }else
                        readBuf.position(readBuf.position() + 8);
                }else{
                    readBuf.position(readBuf.position() + 8 + 8 + 8);
                }
            }else if(conditionFlag == 2){
				readBuf.position(readBuf.position() + len + 8 + 8 + 8);
				//continue;
			}else if(conditionFlag == 1){
				//read the key
				key = new byte [len];
				//stringReadBuf.get(key);
                readBuf.get(key);
                
				if(isSelected(selectCols, key) == true){ //should select this field
					pos = readBuf.getLong();
                    valnum = readBuf.getLong();
                    boolnum = readBuf.getLong();
                    
                    if(pos == UNDEFINED){ //this is not a string
                        if(valnum == UNDEFINED){ // this is a bool
                            result.addLong("", oid, boolnum);
                        }else{ // this is a long
                            result.addLong("", oid, valnum);
                        }
                    }else{
                        result.addLong("", oid, pos);
                    }
                }else
					readBuf.position(readBuf.position() + 8 + 8 + 8);
				//continue;
			}
		}

        
        return;
    }
    
    private boolean isSelected(byte[][] cols, byte[] key){
        
        if(cols[0][0]==(byte) '*' ){//select all fields
            return true;
        }
        
        for(int i = 0; i < cols.length; i++){
            //String keyStr = new String(key);
            //if(keyStr.equals( new String(cols[i]))){
            if(Arrays.equals(key, cols[i])==true){
                return true;
            }
        }
        return false;
    }
    
    /* TODO: Finish this
     select x,y,z where a = ANY xx
     * xx is a set, single column and single relation parsing
     * Method:  there are multiple columns for this set xx,
     *         scan all of where columns and find the oid which meets the condition
     *               for each selected oid, get the values from select columns
     *        assume string type for now
     */
    public HashMap<Integer, HashMap<String, String>>  selectWhereAny(byte[][] selectCols, byte[] whereCol, String relation, byte[] value){
        HashMap<Integer, HashMap<String, String>> resultSet= new HashMap<Integer, HashMap<String, String>>();
        return resultSet;
    }
    
    
    /* TODO: Finish this
     select x,y,z where a = xx or a < xx or a > xx
     * single column and single relation parsing
     * Method: scan the where column and find the oid which meets the condition
     *               for each selected oid, get the values from select columns
     */
    public HashMap<Integer, HashMap<String, String>> selectWhereSingle(byte[][] selectCols, byte[] whereCol, String relation, byte[] value){
        HashMap<Integer, HashMap<String, String>> resultSet= new HashMap<Integer, HashMap<String, String>>();
        return resultSet;
    }
    
    public void printBriefStats(){
        result.printBriefStats();
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

		//JsonReader reader = Json.createReader(new FileReader("testjson/test.json")); 
/*		JsonReader reader = Json.createReader(new FileReader("testjson/abc.json")); 
		JsonObject jsonob = reader.readObject();
		System.out.println(jsonob.toString());

		Argo1 store= new Argo1(100*1000*1000);
		int objid = 1;
		store.insertRow(objid,jsonob,null);
		store.insertRow(2,jsonob,null);
		store.insertRow(3,jsonob,null);

		System.out.println("get the result out \n");
		/* objid,keystr,valstr,valnum,valbool - 5 bytes */

		/* read it out */
/*		store.getRow(1);

        // aggregate scan   
        String targetColumn = "B";
        long sum = store.aggregate(targetColumn.getBytes(),10);
        System.out.println("Aggregate sum Results : "+sum);
*/
	}
}
