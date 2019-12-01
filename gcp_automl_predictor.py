import os
import glob
import pandas as pd
import dask.dataframe as dd

from google.api_core.client_options import ClientOptions
from google.cloud import automl_v1beta1
from google.cloud.automl_v1beta1.proto import service_pb2
from google.cloud import storage

class AutoMLPredictor:

  def __init__(self, model_name):
    self.client = storage.Client()
    self.model_name = model_name

  def __inline_text_payload(self, content):
    return {'text_snippet': {'content': content, 'mime_type': 'text/plain'} }

  def get_prediction(self, text, model_name=None):
    if model_name is None:
      model_name = self.model_name

    options = ClientOptions(api_endpoint='automl.googleapis.com')
    prediction_client = automl_v1beta1.PredictionServiceClient(client_options=options)

    payload = self.__inline_text_payload(text)

    params = {}
    request = prediction_client.predict(model_name, payload, params)
    return request.payload[0].text_sentiment.sentiment  # waits until request is returned


def main():
  pass

if __name__ == "__main__":
    main()
