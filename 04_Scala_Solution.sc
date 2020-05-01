##### Script Scala                                                                
                                                                                  
#variables                                                                        
sourceFilesPath = "/tmp/NASA/"                                                   
                                                                                  
#imports                                                                          
from pyspark.sql import *                                                         
from datetime import datetime                                                     
from operator import add                                                          
                                                                                  
conf = (SparkConf()                                                               
         .setMaster("local")                                                      
         .setAppName("SPark")                                                     
         .set("spark.executor.memory", "2g"))                                     
sc = SparkContext(conf = conf)                                                    
                                                                                  
sourceDf = spark.read.format('text')
          .option("sep"\\t)                        
          .load(sourceFilesPath)
                                                                       
InputFile = sc.sourceDf('final_file')                                             
InputFile = InputFile.cache()                                                     
                                                                                  
# Número de hosts únicos                                                          
host_count = InputFile.flatMap(lambda line: line.split(' ')[0]).distinct().count()
print(host_count)                                                                 
                                                                                  
# O total de erros 404                                                            
def response_code_404(line):                                                      
    try:                                                                          
        code = line.split(' ')[-2]                                                
        if code == '404':                                                         
            return True                                                           
    except:                                                                       
        pass                                                                      
    return False                                                                  
                                                                                  
count_404 = july.filter(response_code_404).cache()                                
print(july_404.count())                                                           
                                                                                  
# Os 5 URLs que mais causaram erro 404                                            
def top5_endpoints(rdd):                                                          
    endpoints = rdd.map(lambda line: line.split('"')[1].split(' ')[1])            
    counts = endpoints.map(lambda endpoint: (endpoint, 1)).reduceByKey(add)       
    top = counts.sortBy(lambda pair: -pair[1]).take(5)                            
                                                                                  
    for endpoint, count in top:                                                   
        print(endpoint, count)                                                    
                                                                                  
    return top                                                                    
                                                                                  
top5_endpoints(count_404)                                                         
print(top5_endpoints.count())                                                     
                                                                                  
# Quantidade de erros 404 por dia                                                 
def daily_count(rdd):                                                             
    days = rdd.map(lambda line: line.split('[')[1].split(':')[0])                 
    counts = days.map(lambda day: (day, 1)).reduceByKey(add).collect()            
                                                                                  
    for day, count in counts:                                                     
        print(day, count)                                                         
                                                                                  
    return counts                                                                 
                                                                                  
daily_count(count_404)                                                            
print(daily_count.count())                                                        
                                                                                  
# O total de bytes retornados                                                     
def accumulated_byte_count(rdd):                                                  
    def byte_count(line):                                                         
        try:                                                                      
            count = int(line.split(" ")[-1])                                      
            if count < 0:                                                         
                raise ValueError()                                                
            return count                                                          
        except:                                                                   
            return 0                                                              
                                                                                  
    count = rdd.map(byte_count).reduce(add)                                       
    return count                                                                  
                                                                                  
print(accumulated_byte_count(InputFile))                                          
