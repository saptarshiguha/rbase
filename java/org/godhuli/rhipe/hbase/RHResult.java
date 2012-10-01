package org.godhuli.rhipe.Rbase;

import java.util.Set;
import java.util.NavigableMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import org.godhuli.rhipe.REXPProtos.REXP;
import org.godhuli.rhipe.REXPProtos;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Result;
import org.godhuli.rhipe.RObjects;
import org.godhuli.rhipe.REXPProtos.REXP;

public class RHResult {

    private Result _result;
    private static REXP template;
    private static String[] _type = new String[]{};

    {
	REXP.Builder templatebuild  = REXP.newBuilder();
	templatebuild.setRclass(REXP.RClass.LIST);
	template = templatebuild.build();
    } 

    public RHResult(){
    }

    REXP.Builder template(){
	return( REXP.newBuilder(template));
    }
    public REXP makeRObject(Result r){
	NavigableMap<byte[],NavigableMap<byte[],byte[]>> map = r.getNoVersionMap();
	ArrayList<String> names = new ArrayList<String>();
	REXP.Builder b = REXP.newBuilder(template);
	for(Map.Entry<byte[] , NavigableMap<byte[],byte[]> > entry: map.entrySet()){
	    String family = new String(entry.getKey());
	    for(Map.Entry<byte[], byte[]> columns : entry.getValue().entrySet()){
		String column = new String(columns.getKey());
		names.add( family +":"+column);
		REXP.Builder thevals   = REXP.newBuilder();
		thevals.setRclass(REXP.RClass.RAW);
		thevals.setRawValue(com.google.protobuf.ByteString.copyFrom( columns.getValue() ));
		b.addRexpValue( thevals.build() );
	    }
	}
	b.addAttrName("names");
	b.addAttrValue(RObjects.makeStringVector(names.toArray(_type)));
	return(b.build());
    }



}
