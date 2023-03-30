from boto3 import Session
import datetime

class logs:

    def __init__(self, job,profileName):
        self.job = job
        # self.job_id=job_run_id
        self.profileName=profileName

    def _connected_client(self):
        
        """ Connect to logs"""
        session = Session(profile_name=self.profileName)
        return session.client('logs')
    
    def stream_name(self):
        return self.stream
    
    def get_logs(self):
        
        print(self.job)
        
        response= self._connected_client().start_query(logGroupName="/aws-glue/jobs/error",
                                                     startTime=int((datetime.datetime.now()-datetime.timedelta(hours=1)).timestamp()),
                                                     endTime=int(datetime.datetime.now().timestamp()),
                                                     queryString='fields @timestamp, message | filter message like \'%s\' | sort @timestamp desc | limit 1' % self.job)
        query_id=response['queryId']
        response=None
        while response== None or response['status'] == 'Running':
            print("Query in progress...")
            response=self._connected_client().get_query_results(queryId=query_id)
        
        job_run_id=None
        
        print(response)
        
        
        for field in response['results'][0]:
            if field['field']== "message":
                log_message=field['value']
                
                if self.job in log_message:
                    job_run_id= log_message.split(self.job)[-1].split(' ')[1]
                    break
                
        if job_run_id is None:
            print ("Unable to find latest job run")
            exit        
        
        response= self._connected_client.filter_log_events(logGroupName="/aws-glue/jobs/error",
                                                           filterPattern='[aws-glue] %s[%s]' % (self.job,job_run_id))
        
        for event in response['events']:
            print(event['message']) 
                           
        
        
        # log_group_name="/aws-glue/jobs/error"
        
        # print("log group name {}",log_group_name)
        # log_stream_name= f'{self.job_id}'
        
        # print("Log stream name {}".format(log_stream_name))
        # logs= self._connected_client()
        
        
        # response= logs.get_log_events(logGroupName=log_group_name,logStreamName=log_stream_name)
        
        # for event in response['events']:
          
        #      print(event['message'])
    
    