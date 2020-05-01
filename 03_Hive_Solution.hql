##### Segunda opção - script Hive                                                                                       
                                                                                                                        
/usr/bin/hive -S -e "CREATE TABLE IF NOT EXISTS database.NASA_access_log(                                               
  host_requisition string,                                                                                              
  timestamp_requisition string,                                                                                         
  requisition string,                                                                                                   
  return_code_http string,                                                                                              
  bytes_returned string)                                                                                                
ROW FORMAT DELIMITED                                                                                                    
  FIELDS TERMINATED BY '\040'                                                                                           
STORED AS INPUTFORMAT                                                                                                   
  'org.apache.hadoop.mapred.TextInputFormat'                                                                            
OUTPUTFORMAT                                                                                                            
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';"                                                        
                                                                                                                        
/usr/bin/hive -e "database.NASA_access_log;load data inpath '/home/user/final_file' into table database.NASA_access_log"
                                                                                                                        
/usr/bin/hive -e "select count(distinct host_requisition) as host_requisition from database.NASA_access_log;"           
                                                                                                                        
/usr/bin/hive -e "select count(*) as errors_404 from database.NASA_access_log where return_code_http = '404';"          
                                                                                                                        
/usr/bin/hive -e "select requisition, count(*) as cnt_404                                                               
                    from database.NASA_access_log                                                                       
                   WHERE where return_code_http = '404'                                                                 
                   group by requisition                                                                                 
                   order by 2 desc                                                                                      
                   limit 5;"                                                                                            
                                                                                                                        
/usr/bin/hive -e "select cast(timestamp_requisition as date) as dia, count(*) as errors_404                             
                    from database.NASA_access_log                                                                       
                   where return_code_http = '404'                                                                       
                   group by cast(timestamp_requisition as date);"                                                       
                                                                                                                        
/usr/bin/hive -e "select sum(cast(bytes_returned as bigint)) as total_bytes from database.NASA_access_log;"             
#####    
