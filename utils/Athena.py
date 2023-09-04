from boto3 import Session


class Athena:

    def __init__(self, profile_name, database, output_bucket, output_folder):
        self.profileName = profile_name
        self.database = database
        self.bucket = output_bucket
        self.folder = output_folder

    def client(self):
        session = Session(profile_name=self.profileName)
        return session.client('athena')

    def run_query(self, query):
        # print("QUERY IS", query)
        response = self.client().start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': self.database
            },
            ResultConfiguration={
                'OutputLocation': f's3://{self.bucket}/{self.folder}/'
            }
        )

        query_execution_id = response['QueryExecutionId']

        while True:
            query_execution = self.client().get_query_execution(
                QueryExecutionId=query_execution_id)
            status = query_execution['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break

            # Check if the query succeeded
        if status == 'SUCCEEDED':
            # Get the query results
            results = self.client().get_query_results(
                QueryExecutionId=query_execution_id)

            column_names = [datum['VarCharValue']
                            for datum in results['ResultSet']['Rows'][0]['Data']]

            data_rows = results['ResultSet']['Rows'][1:]
            data = []

            for row in data_rows:
                data_row = {}
                for i, datum in enumerate(row['Data']):
                    data_row[column_names[i]] = datum.get('VarCharValue')
                data.append(data_row)
            return data

        else:

            message = query_execution['QueryExecution']['Status']['StateChangeReason']
            return message
