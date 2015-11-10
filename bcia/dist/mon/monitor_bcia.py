#!/usr/bin/python
# -*- coding:utf8 -*-
import MySQLdb
import time
import datetime
import os

def bciaMysql(sql):
    try:
        conn = MySQLdb.connect (host = "r2s4", user = "bcia", passwd = "bcia", db = "bcia_statis")  
        cursor = conn.cursor ()  
        cursor.execute (sql)  
        result = cursor.fetchall ()  
        return result
        cursor.close ()  
        conn.close ()
    except MySQLdb.Error,e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])

if __name__ =="__main__":
    d_time=time.strftime('%Y-%m-%d %H:%M:%S')
    current_time=time.mktime(time.strptime(d_time[0:15]+"0:00",'%Y-%m-%d %H:%M:%S'))-600
    current_d=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(current_time))
    d_current_time=time.mktime(time.strptime(current_d,'%Y-%m-%d %H:%M:%S'))    

    sql_wifi="select unix_timestamp(segmt) from wifi_result_segmt order by segmt desc limit 1"
    sql_aj="select unix_timestamp(segmt) from profession_result_segmt order by segmt desc limit 1"
    wifi_db=long(bciaMysql(sql_wifi)[0][0])
    wifi_db_d=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(wifi_db))
    aj_db=long(bciaMysql(sql_aj)[0][0])
    aj_db_d=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(aj_db))

    aj_path="/mnt/bigpfs/bcia-queue/calc/anjian/dura/"
    wifi_path="/mnt/bigpfs/bcia-queue/calc/wifi/dura/"
    aj_duradir=max(os.listdir(aj_path))
    wifi_duradir=max(os.listdir(wifi_path))
    aj_durafile=os.listdir(aj_path+max(os.listdir(aj_path)))
    wifi_durafile=os.listdir(wifi_path+max(os.listdir(wifi_path)))
    for i in aj_durafile:
        durafile=open(aj_path+aj_duradir+"/"+i)
        for j in durafile:    
            aj_hdfs=time.mktime(time.strptime(j.split("\t")[0],'%Y-%m-%d %H:%M'))
            aj_hdfs_d=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(aj_hdfs))
        durafile.close()
    for i in wifi_durafile:
        durafile=open(wifi_path+wifi_duradir+"/"+i)
        for j in durafile:
            wifi_hdfs=time.mktime(time.strptime(j.split("\t")[0],'%Y-%m-%d %H:%M'))
            wifi_hdfs_d=time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(wifi_hdfs))
        durafile.close()

#    lkxxb_path="/mnt/bigpfs/bcia-queue/calc/anjian/lkxxb/"
#    barcode_path="/mnt/bigpfs/bcia-queue/calc/anjian/barcode/"
#    ajxxb_path="/mnt/bigpfs/bcia-queue/calc/anjian/ajxxb/"
#    lkxxb_duradir=max(os.listdir(lkxxb_path))
#    barcode_duradir=max(os.listdir(barcode_path))
#    ajxxb_duradir=max(os.listdir(ajxxb_path))
#    lkxxb_durafile=os.listdir(lkxxb_path+max(os.listdir(lkxxb_path)))
#    barcode_durafile=os.listdir(barcode_path+max(os.listdir(barcode_path)))
#    ajxxb_durafile=os.listdir(ajxxb_path+max(os.listdir(ajxxb_path)))
#    lkxxb_list=[]
#    barcode_list=[]
#    ajxxb_list=[]
#    for i in lkxxb_durafile:
#        durafile=open(lkxxb_path+lkxxb_duradir+"/"+i)
#        for j in durafile:
#            lkxxb_list.append(j.strip().split("\t")[-1])
#    for i in barcode_durafile:
#        durafile=open(barcode_path+barcode_duradir+"/"+i)
#        for j in durafile:
#            barcode_list.append(j.strip().split("\t")[-1])
#    for i in ajxxb_durafile:
#        durafile=open(ajxxb_path+ajxxb_duradir+"/"+i)
#        for j in durafile:
#            ajxxb_list.append(j.strip().split("\t")[-1])
    lkxxb_file=open('/mnt/bigpfs/bcia-queue/conf/sql-lkxxb.ctl')
    for i in lkxxb_file:
        lkxxb_hdfs_e=i.strip().split("\t")
        if len(lkxxb_hdfs_e)==3:
            lkxxb_hdfs_d=lkxxb_hdfs_e[2] 
            lkxxb_hdfs=time.mktime(time.strptime(lkxxb_hdfs_d,'%Y-%m-%d %H:%M:%S'))
    lkxxb_file.close()
    barcode_file=open("/mnt/bigpfs/bcia-queue/conf/sql-barcode.ctl")
    for i in barcode_file:
        barcode_hdfs_e=i.strip().split("\t")
        if len(barcode_hdfs_e)==3:
            barcode_hdfs_d=barcode_hdfs_e[2]
            barcode_hdfs=time.mktime(time.strptime(barcode_hdfs_d,'%Y-%m-%d %H:%M:%S'))
    barcode_file.close()
    ajxxb_file=open("/mnt/bigpfs/bcia-queue/conf/sql-ajxxb.ctl")
    for i in ajxxb_file:
        ajxxb_hdfs_e=i.strip().split("\t")
        if len(ajxxb_hdfs_e)==3:
            ajxxb_hdfs_d=ajxxb_hdfs_e[2]
            ajxxb_hdfs=time.mktime(time.strptime(ajxxb_hdfs_d,'%Y-%m-%d %H:%M:%S'))
    ajxxb_file.close()

    if d_current_time-lkxxb_hdfs<300:
        lkxxb_status="normal"
    elif d_current_time-lkxxb_hdfs>=300 and d_current_time-lkxxb_hdfs<600:
        lkxxb_status="warn"
    else:
        lkxxb_status="faild"

    if d_current_time-barcode_hdfs<300:
        barcode_status="normal"
    elif d_current_time-barcode_hdfs>=300 and d_current_time-barcode_hdfs<600:
        barcode_status="warn"
    else:
        barcode_status="faild"

    if d_current_time-ajxxb_hdfs<300:
        ajxxb_status="normal"
    elif d_current_time-ajxxb_hdfs>=300 and d_current_time-ajxxb_hdfs<600:
        ajxxb_status="warn"
    else:
        ajxxb_status="faild"

    if current_time-aj_db<1200:
        ajdb_status="normal"
    else:
        ajdb_status="faild"

    if current_time-wifi_db<1200:
        wifidb_status="normal"
    else:
        wifidb_status="faild"

    if current_time-aj_hdfs<1200:
        ajhdfs_status="normal"
    else:
        ajhdfs_status="faild"

    if current_time-wifi_hdfs<1200:
        wifihdfs_status="normal"
    else:
        wifihdfs_status="faild"

    if lkxxb_status=="normal" and barcode_status=="normal" and ajxxb_status=="normal" and ajdb_status=="normal" and wifidb_status=="normal" and ajhdfs_status=="normal" and wifihdfs_status=="normal":
        status="Normal"
    else:
        status="Faild"

#print "status: "+status+"\n\ndata intest\n=========================\nlkxxb    "+lkxxb_hdfs_d+"	"+d_time+"     "+lkxxb_status+"\nbarcode  "+barcode_hdfs_d+"	"+d_time+"     "+barcode_status+"\najxxb    "+ajxxb_hdfs_d+"	"+d_time+"     "+ajxxb_status+"\n\ndata statis\n=========================\nanjian_db        "+aj_db_d+"	"+current_d+"     "+ajdb_status+"\nanjian_hdfs     "+aj_hdfs_d+"	"+current_d+"     "+ajhdfs_status+"\nwifi_db  "+wifi_db_d+"	"+current_d+"     "+wifidb_status+"\nwifi_hdfs        "+wifi_hdfs_d+"	"+current_d+"     "+wifihdfs_status+""
print "status: "+status
print ""
print "data intest"
print "========================="
print "lkxxb    "+lkxxb_hdfs_d+"  "+d_time+"     "+lkxxb_status
print "barcode  "+barcode_hdfs_d+"    "+d_time+"     "+barcode_status
print "ajxxb    "+ajxxb_hdfs_d+"    "+d_time+"     "+ajxxb_status
print ""
print "data statis"
print "========================="
print "anjian_db        "+aj_db_d+" "+current_d+"     "+ajdb_status
print "anjian_hdfs     "+aj_hdfs_d+"        "+current_d+"     "+ajhdfs_status
print "wifi_db  "+wifi_db_d+"     "+current_d+"     "+wifidb_status
print "wifi_hdfs        "+wifi_hdfs_d+"   "+current_d+"     "+wifihdfs_status
