from boto3 import Session


class Logs:

    def __init__(self, job_name, profile_name, filter_events="ERROR", tail=False):
        self.job = job_name
        self.profileName = profile_name
        self.tail = tail
        self.logGroupName = "/aws-glue/jobs/error"
        self.filter_events = filter_events
        self.run_id = []

    def _connected_clients(self):
        self.resources = {}
        session = Session(profile_name=self.profileName)
        self.resources = {"glue": session.client('glue'), "logs": session.client('logs')}

        print(self.resources)
        return self.resources

    def get_glue_logs(self):

        glue_response = self._connected_clients().get("glue").get_job_runs(
            JobName=self.job,
            MaxResults=100
        )

        for job_run in glue_response['JobRuns']:
            self.run_id.append(job_run['Id'])

        log_response = self._connected_clients().get('logs').filter_log_events(
            logGroupName=self.logGroupName,
            logStreamNames=[self.run_id[0]],
            filterPattern=self.filter_events)

        for event in log_response['events']:
            print(event['message'])
