from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from datetime import datetime

import time
import random
import requests

from concurrent import futures

CALLBACK_URL = "https://127.0.0.1:8080/"

executor = futures.ThreadPoolExecutor(max_workers=1)

def calculate_data_growth(growth_request, factors, moderator_id):
    data_val = sum([f["FactorNum"] * f["DataGrowthFactor"]["coeff"] for f in factors])
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    start_period = datetime.strptime(growth_request["StartPeriod"], fmt)
    end_period = datetime.strptime(growth_request["EndPeriod"], fmt)
    duration = (end_period - start_period).days
    result = growth_request["CurData"] + data_val * duration
    time.sleep(5)

    return {
      "id": growth_request["ID"],
      "res": result,
      "moderator_id": moderator_id,
    }

def status_callback(task):
    def_token = "ABCDEF12"
    try:
      result = task.result()
    except futures._base.CancelledError:
      return
    
    nurl = str(CALLBACK_URL+'api/growth-requests/'+str(result["id"])+'/result')
    answer = {"result": result["res"], "token": def_token, "moderator_id": result["moderator_id"]}
    requests.put(nurl, json=answer, timeout=3, verify="localhost+2.pem")


@api_view(['POST'])
def set_status(request):
    if {"growth_request", "factors"} <= set(request.data.keys()):   
        growth_request = request.data["growth_request"] 
        factors = request.data["factors"]  
        moderator_id = request.data["moderator_id"]

        task = executor.submit(calculate_data_growth, growth_request, factors, moderator_id)
        task.add_done_callback(status_callback)        
        return Response(status=status.HTTP_200_OK)
    return Response(status=status.HTTP_400_BAD_REQUEST)