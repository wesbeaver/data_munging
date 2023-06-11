import argparse
import datetime

from apiclient.discovery import build
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools
import concurrent.futures
import pandas as pd


class GoogleAnalyticsReporting:
    def __init__(self,view_id,secret_file_path):
        self.secret_file_path = secret_file_path
        self.SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
        self.DISCOVERY_URI = ('https://analyticsreporting.googleapis.com/$discovery/rest')
        self.CLIENT_SECRETS_PATH = secret_file_path
        self.VIEW_ID = view_df
    
    def initialize_analyticsreporting(self):
        """Initializes the analyticsreporting service object.

      Returns:
        analytics an authorized analyticsreporting service object.
      """
      # Parse command-line arguments.
        parser = argparse.ArgumentParser(
          formatter_class=argparse.RawDescriptionHelpFormatter,
          parents=[tools.argparser])
        flags = parser.parse_args([])

      # Set up a Flow object to be used if we need to authenticate.
        flow = client.flow_from_clientsecrets(
          self.CLIENT_SECRETS_PATH, scope=self.SCOPES,
          message=tools.message_if_missing(self.CLIENT_SECRETS_PATH))

      # Prepare credentials, and authorize HTTP object with them.
      # If the credentials don't exist or are invalid run through the native client
      # flow. The Storage object will ensure that if successful the good
      # credentials will get written back to a file.
        storage = file.Storage('analyticsreporting.dat')
        credentials = storage.get()
        if credentials is None or credentials.invalid:
            credentials = tools.run_flow(flow, storage, flags)
        http = credentials.authorize(http=httplib2.Http())

      # Build the service object.
        analytics = build('analytics', 'v4', http=http, discoveryServiceUrl=self.DISCOVERY_URI)

        return analytics

    def create_dimensions(self,dimension_list):
        dfinal_list = [ dict().fromkeys(['name'],i) for i in dimension_list]
        self.dfinal_list = dfinal_list
        return dfinal_list

    def create_metrics(self,metric_list):
        mfinal_list = [ dict().fromkeys(['expression'],i) for i in metric_list]
        return mfinal_list

    def date_range(self,start_finish_list):
        start = datetime.datetime.strptime(start_finish_list[0], "%Y-%m-%d")
        end = datetime.datetime.strptime(start_finish_list[1], "%Y-%m-%d")
        print(end)
        date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days)]
        return date_generated
    
    def configure_report(self,dimension_list,metric_list):
        dfinal_lilst = self.create_dimensions(dimension_list)
        mfinal_list = self.create_metrics(metric_list)
        date_generated = self.date_range(start_finish_list)
        return dfinal_list,mfinal_list

    def get_report(self,analytics,dimension_list,metric_list,date):
        dfinal_list,mfinal_list = self.configure_report(dimension_list,metric_list)
        return analytics.reports().batchGet(
              body={
                'reportRequests': [
                {
                  'viewId': self.VIEW_ID,
                  'dateRanges': [{'startDate': date, 'endDate': date}],
                  "dimensions": dfinal_list,
                  'metrics': mfinal_list
                }]
              }
          ).execute()


    def json_to_dataframe(json_data):
        column_dict = {}
        for j in range(0, len(json_data)):
            if j == 0:
                for i in json_data[j]['reports'][0]['columnHeader']['metricHeader']['metricHeaderEntries']:
                    keyname = i['name']
                    column_dict[keyname] = []

                for i in json_data[j]['reports'][0]['columnHeader']['dimensions']:
                    keyname = i
                    column_dict[keyname] = []
                    print('yes')
            elif j>=1:
                for i in json_data[j]['reports'][0]['data']['rows']:
                    for j,k in enumerate(i['dimensions']):
                        keyname = dimension[j]
                        column_dict[keyname].append(k)
                    for e,f in enumerate(i['metrics'][0]['values']):
                        keyname = metrics[e]['name']
                        column_dict[keyname].append(f)
        results_df = pd.DataFrame(column_dict)
        return results_df

class UnrollAggregate:
    def __init__(self,data,reference_column:str,target_column:str,is_conversion=None):
        assert reference_column in data.columns,\
            "Reference column %s is not in dataframe columns %s" % (reference_column,data.columns)
        assert target_column in data.columns,\
            "Target column %s is not in dataframe columns %s" % (target_column,data.columns)
        self.data = data
        self.reference_column = reference_column
        self.target_column = target_column
        self.is_conversion = is_conversion
        self.starting_dict = {l:[] for l in data.columns if l != reference_column}
        
    def process_independent(self):
        for i in range(0,len(self.data)):
            rows_to_produce = data.iloc[i:i+1].to_dict()[self.reference_column][i]
            targets_to_produce = data.iloc[i:i+1].to_dict()[self.target_column][i]
            for j in range(rows_to_produce):
                target_value = 1 if j < targets_to_produce else 0
                for k in self.starting_dict.keys():
                    if k == self.target_column:
                        if self.is_conversion==True:
                            self.starting_dict[k].append(target_value)
                        else:
                            self.starting_dict[k].append(targets_to_produce/rows_to_produce)
                            
                    else:
                        self.starting_dict[k].append(data.iloc[i:i+1].to_dict()[k][i])
        return self.starting_dict
    
    def process_row(self,i):
        rows_to_produce = self.data.iloc[i:i+1].to_dict()[self.reference_column][i]
        targets_to_produce = self.data.iloc[i:i+1].to_dict()[self.target_column][i]
        for j in range(rows_to_produce):
            target_value = 1 if j < targets_to_produce else 0
            for k in self.starting_dict.keys():
                if k == self.target_column:
                    if self.is_conversion==True:
                        self.starting_dict[k].append(target_value)
                    else:
                        self.starting_dict[k].append(targets_to_produce/rows_to_produce)

                else:
                    self.starting_dict[k].append(data.iloc[i:i+1].to_dict()[k][i])
    
    def process_in_parallel(self):
        with concurrent.futures.ProcessPoolExecutor() as executor:
            executor.map(self.process_row,[i for i in range(len(self.data))])
        return self.starting_dict
            #target_to_product = data.iloc[i:i+1].to_dict(target_column)[i]