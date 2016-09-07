##problem statement :generting session id for the logs and generate quantative metrics


library(dplyr)
library(data.table)
library(stringr)

##load data from file delimited by ' ' 
dt<-read.table('2015_07_22_mktplace_shop_web_log_sample.log',sep=' ',header=F,quote='"',colClasses=c("character"),fill=T)
names(dt)<-c("timestamp","elb","client_port","backend_port","request_processing_time","backend_processing_time","response_processing_time","elb_status_code","backend_status_code","received_bytes","sent_bytes","request","user_agent","ssl_cipher","ssl_protocol")

##extract client ip from client port
dt$client_ip<-sapply(strsplit(dt$client_port,':'),'[',1) 

##extract url from request ignoring protocol and type of request to be used later in computing unique urls visited
url_pattern <- "http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"
dt$url<-str_extract(dt$request,url_pattern)


dt<-data.table(dt)
##Data type conversion to time stamp
dt[, event_timestamp:=as.POSIXct(strptime(timestamp,"%Y-%m-%dT%H:%M:%OS"))]

## Order by uid and event_timestamp:
setkey(dt, client_ip, event_timestamp)


## Sessionize the data (more than 30 minutes between events is a new session):
dt<-data.table(dt)
##computes for each userid ,difference between current event and last event
##if difference is greater than 30 - create a new sessionid as userid_visitnumber
dt[, session_id:=paste(client_ip, cumsum((c(0, difftime(event_timestamp,lag(event_timestamp),units=c("secs"))[-1])/60 > 30)*1), sep="_"), by=client_ip]



##Compute basic session time elapsed statistics and number of unique urls visited in each session
time_elapsed_session<-dt %>% group_by(session_id) %>% dplyr::summarize(time_elapsed=difftime(max(event_timestamp),min(event_timestamp),units=c("secs")),num_unique_url=n_distinct(url))


time_elapsed_session<-data.frame(time_elapsed_session)


##average session length
mean(time_elapsed_session$time_elapsed,na.rm=T)




##find the top k engaged users

time_elapsed_client<-dt %>% group_by(client_ip,session_id) %>% dplyr::summarize(time_elapsed=difftime(max(event_timestamp),min(event_timestamp),units=c("secs"))) %>% group_by(client_ip) %>% dplyr::summarize(total_time_Spent=sum(time_elapsed))

time_elapsed_client<-data.frame(time_elapsed_client)
k<-10
time_elapsed_client$client_ip[order(time_elapsed_client$total_time_Spent,decreasing=T)[1:k]]





