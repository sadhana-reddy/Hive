package com.sadhana.hiveudf;

import java.util.ArrayList;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import com.sun.tools.javac.util.List;


	public class ContainsString extends GenericUDF {

		  ListObjectInspector listOI;
		  StringObjectInspector elementOI;

		  @Override
		  public String getDisplayString(String[] arg0) {
		    return "arrayContainsExample()"; 
		  }

		  @Override
		  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		    if (arguments.length != 2) {
		      throw new UDFArgumentLengthException("arrayContainsExample only takes 2 arguments: List<T>, T");
		    }
			//check if the fiest argument is list and second is string
		    ObjectInspector l = arguments[0];
		    ObjectInspector str = arguments[1];
		    if (!(l instanceof ListObjectInspector) || !(str instanceof StringObjectInspector)) {
		      throw new UDFArgumentException("first argument must be a list / array, second argument must be a string");
		    }
		    this.listOI = (ListObjectInspector) l;
		    this.elementOI = (StringObjectInspector) str;
		    
		    // Check if the list contains strings
		    if(!(listOI.getListElementObjectInspector() instanceof StringObjectInspector)) {
		      throw new UDFArgumentException("first argument must be a list of strings");
		    }
		    
		    // the return type is a boolean
		    return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
		  }
		  
		  @Override
		  public Object evaluate(DeferredObject[] arguments) throws HiveException {
		    
		    // get the list and string from the deferred objects using the object inspectors
		    ArrayList<String> list = (ArrayList<String>)this.listOI.getList(arguments[0].get());
		    String arg = elementOI.getPrimitiveJavaObject(arguments[1].get());
		    
		    // Check if nulls exist
		    if (list == null || arg == null) {
		      return null;
		    }
		    
		    // see if list contains the required string
		    for(String s: list) {
		      if (arg.equals(s)) return new Boolean(true);
		    }
		    return new Boolean(false);
		  }
		  
		}

