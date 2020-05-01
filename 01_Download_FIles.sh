                                                                                     
##### Shell Script - recupera, padroniza e une os arquivos                                        
                                                                                                  
cd /tmp/NASA/

curl l ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz >> NASA_access_log_Jul95.gz
curl l ftp://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz >> NASA_access_log_Aug95.gz
                                                                                                  
                                                                                     
gzip -d  NASA_access_log_Jul95.gz                                                                 
gzip -d  NASA_access_log_Aug95.gz                                                                 
                                                                                                  
cat NASA_access_log_Jul95   | sed 's/^//g'    > NASA_access_log_Jul95_1                          
cat NASA_access_log_Jul95_1 | sed 's/\[/\"/g' > NASA_access_log_Jul95_2                          
cat NASA_access_log_Jul95_2 | sed 's/\]/\"/g' > NASA_access_log_Jul95_3                       
                                                                                                  
cat NASA_access_log_Aug95   | sed 's/^//g'    > NASA_access_log_Aug95_1                          
cat NASA_access_log_Aug95_1 | sed 's/\[/\"/g' > NASA_access_log_Aug95_2                          
cat NASA_access_log_Aug95_2 | sed 's/\]/\"/g' > NASA_access_log_Aug95_3                          
                                                                                                  
cat NASA_access_log_Jul95_3 > NASA_access_log                                                          
cat NASA_access_log_Aug95_3 >> NASA_access_log     

