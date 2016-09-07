register datafu-pig-incubating-1.3.1.jar
register piggybank-0.11.0.jar
DEFINE ISOToUnix   org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();
DEFINE Sessionize  datafu.pig.sessions.Sessionize('30m');
DEFINE Median      datafu.pig.stats.Median();
DEFINE Quantile    datafu.pig.stats.StreamingQuantile('0.75','0.90','0.95','1');
DEFINE VAR         datafu.pig.stats.VAR();
 data = LOAD '/app/personalization/2015_07_22_mktplace_shop_web_log_sample.log' USING  org.apache.pig.piggybank.storage.CSVExcelStorage(' ') AS ( timestamp:chararray, elb:chararray,client_port:chararray,backend_port:chararray,request_processing_time:float,backend_processing_time:float,response_processing_time:float,elb_status_code:int,backend_status_code:int,received_bytes:int,sent_bytes:int,request:chararray,user_agent:chararray,ssl_cipher:chararray,ssl_protocol:chararray);
DESCRIBE data
b = FOREACH data GENERATE ISOToUnix(timestamp) as isoTime,
              timestamp,
              flatten(STRSPLIT(client_port, ':')) as (ip:chararray, port:chararray),
              flatten(STRSPLIT(request, 'HTTP/')) as (url:chararray, version:chararray);

grouped = GROUP b BY ip;
c = FOREACH grouped {
       ordered = ORDER b BY isoTime ;
      GENERATE FLATTEN(Sessionize(ordered)) AS (isoTime,timestamp, ip,port,url,version, session_id);
 };
session_times = FOREACH (GROUP c BY session_id){
				url = DISTINCT c.url;
				ip = DISTINCT c.ip;
				GENERATE group as session_id,
                         (MAX(c.isoTime)-MIN(c.isoTime))
                            / 1000.0 / 60.0 as session_length,
                            FLATTEN(ip), COUNT(url) as unique_urls;
}

total_session_times = FOREACH (GROUP session_times BY ip){
	total_session_time = SUM(session_times.session_length);
	GENERATE FLATTEN(session_times.ip) as ip, total_session_time as total_session_time;
}
total_session_times =  DISTINCT total_session_times;
ordered_total_session_time = ORDER total_session_times BY total_session_time DESC;
most_engaged_users = LIMIT ordered_total_session_time 10;
dump most_engaged_users;

session_stats = FOREACH (GROUP session_times ALL) {
  ordered = ORDER session_times BY session_length;
  GENERATE
    AVG(ordered.session_length) as avg_session,
    Median(ordered.session_length) as median_session,
    Quantile(ordered.session_length) as quantiles_session;
};
session_stats = LIMIT session_stats 10;

dump most_engaged_users;
dump session_stats;
                
