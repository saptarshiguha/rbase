/**
 * Copyright 2010 Mozilla Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.godhuli.rhipe.Rbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

import org.godhuli.rhipe.RObjects;
import org.godhuli.rhipe.REXPProtos.REXP;
import java.util.ArrayList;
import java.io.IOException;


public class Admin {
    private Configuration _cfg;
    private HBaseAdmin _admin;
    private REXP anull;
    private REXP.Builder keycontainer;
    public Admin() throws MasterNotRunningException,ZooKeeperConnectionException{
	_cfg = HBaseConfiguration.create();
	_admin = new HBaseAdmin(_cfg);
	REXP.Builder returnvalue   = REXP.newBuilder();
	returnvalue.setRclass(REXP.RClass.NULLTYPE);
	anull  = returnvalue.build();
	REXP.Builder keycontainer   = REXP.newBuilder();
	keycontainer.setRclass(REXP.RClass.RAW);

    }
    public Admin(String conffile) throws MasterNotRunningException,ZooKeeperConnectionException{
	this(new String[]{conffile});
    }
    public Admin(String[] conffile) throws MasterNotRunningException,ZooKeeperConnectionException{
	_cfg = HBaseConfiguration.create();
	for(String c: conffile){
	    System.err.println("Adding "+c);
	    _cfg.addResource(c);
	}
	_admin = new HBaseAdmin(_cfg);
    }
    public HBaseAdmin getAdmin(){
	return _admin;
    }
    public byte[] listTables() throws IOException {
	HTableDescriptor[] htd = _admin.listTables();
	String[] tbnames = new String[htd.length];
	for(int i= 0;i<tbnames.length;i++){
	    tbnames[i] = htd[i].getNameAsString();
	}
	return(RObjects.makeStringVector(tbnames).toByteArray());
    }

    public void dumpConfiguration() throws IOException{
	java.io.OutputStreamWriter writer = new java.io.OutputStreamWriter(System.out) ;
	Configuration.dumpConfiguration(_cfg, writer);
    }

    public HColumnDescriptor completeColumnDescriptor(HColumnDescriptor cd, String bloomtype, String comptype, String compcompression){
	if(bloomtype != null){
	    if(bloomtype.equals("None"))
		cd.setBloomFilterType(BloomType.NONE);
	    else if(bloomtype.equals("ROW"))
		cd.setBloomFilterType(BloomType.ROW);
	    else if(bloomtype.equals("ROWCOL"))
		cd.setBloomFilterType(BloomType.ROWCOL);
	}
	if(comptype!=null){
	    if(comptype.equals("NONE")) cd.setCompressionType(Compression.Algorithm.NONE);
	    else if (comptype.equals("GZ")) cd.setCompressionType(Compression.Algorithm.GZ);
	    else if (comptype.equals("LZO")) cd.setCompressionType(Compression.Algorithm.LZO);
	    else if (comptype.equals("SNAPPY")) cd.setCompressionType(Compression.Algorithm.SNAPPY);
	}
	if(compcompression!=null){
	    if(compcompression.equals("NONE")) cd.setCompactionCompressionType(Compression.Algorithm.NONE);
	    else if (compcompression.equals("GZ")) cd.setCompactionCompressionType(Compression.Algorithm.GZ);
	    else if (compcompression.equals("LZO")) cd.setCompactionCompressionType(Compression.Algorithm.LZO);
	    else if (compcompression.equals("SNAPPY")) cd.setCompactionCompressionType(Compression.Algorithm.SNAPPY);
	}
	return(cd);
    }

    public void putOne(HTable ht, byte[] key, String[] colfams, byte[][] values) throws IOException{
	Put p = new Put(key);
	for(int i=0;i<colfams.length;i++){
	    String[] cfcq = colfams[i].split(":");
	    p.add(cfcq[0].getBytes(),cfcq[1].getBytes(), values[i]);
	}
	ht.put(p);
    }
    public void putMany(HTable ht, byte[][] keys, String[][] colfams, byte[][][] values) throws IOException{
	ArrayList<Put> listputs =  new ArrayList<Put>( keys.length);
	for(int p=0;p< keys.length;p++){
	    Put pu = new Put(keys[p]);
	    for(int i=0;i<colfams[p].length;i++){
		String[] cfcq = colfams[p][i].split(":");
		pu.add(cfcq[0].getBytes(),cfcq[1].getBytes(), values[p][i]);
	    }
	    listputs.add(pu);
	}
	ht.put(listputs);
    }

    public byte[] getMany(HTable ht, byte[][] keys, String[][] colfams) throws IOException{
	ArrayList<Get> gets =  new ArrayList<Get>( keys.length);
	for(int g=0;g<keys.length;g++){
	    String[] cf  = colfams[g];
	    Get aget = new Get(keys[g]);
	    for(int i=0;i<cf.length;i++){
		String[] fc = cf[i].split(":");
		if(fc.length==1) {
		    aget.addFamily(fc[0].getBytes());//entire family
		} else {
		    aget.addColumn(fc[0].getBytes(),fc[1].getBytes());
		}
	    }
	    gets.add(aget);
	}
	Result[] results = ht.get(gets);
	RHResult r = new RHResult();
	REXP.Builder b = r.template();
	for(Result f: results){
	    REXP.Builder justone = r.template();
	    if(f == null || f.isEmpty()) {
		b.addRexpValue(anull);
	    }else{
		b.addRexpValue( r.makeRObject(f));
	    }
	}
	return(b.build().toByteArray());
    }






}
    





// options(java.parameters="-Xrs")
// library(rJava)
// .jinit()

// HBASE.HOME  = "/usr/lib/hbase"
// HADOOP.HOME = "/usr/lib/hadoop"
// HADOOP.CONF = sprintf("%s/conf",HADOOP.HOME)
// HBASE.CONF  = sprintf("%s/conf",HBASE.HOME)
// rhipeJar    = rhoptions()$jarloc

// hadoopJars <- list.files(HADOOP.HOME,pattern="jar$",full.names=TRUE,rec=TRUE)
// hbaseJars  <- list.files(HBASE.HOME,pattern="jar$",full.names=TRUE,rec=TRUE)
// hadoopConf <- list.files(HADOOP.CONF,pattern="-site.xml$",full.names=TRUE,rec=TRUE)
// hbaseConf  <- list.files(HBASE.CONF,pattern="-site.xml$",full.names=TRUE,rec=TRUE)
// jars <- c(HADOOP.CONF, HBASE.CONF,hadoopJars,hbaseJars,rhipeJar,"/home/sguha/tmp/rbase.jar")
// .jaddClassPath(jars)

// rbadmin <-.jnew("org/godhuli/rhipe/Rbase/Admin")

// rbadmin$listTables()
