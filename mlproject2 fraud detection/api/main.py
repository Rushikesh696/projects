from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
import joblib
import numpy as np

app = FastAPI()                                                                                                                                                                     
                                                                                                                                                                                      
model = joblib.load("fraud_model.pkl")                                                                                                                                              
le = joblib.load("label_encoder.pkl")


class Transaction(BaseModel):                                                                                                                                                       
    transaction_amount: float
    merchant_category: str                                                                                                                                                          
    transaction_hour: int
    day_of_week: int
    device_type: str
    location_city: str                                                                                                                                                              
    distance_from_home: float
    is_new_device: int                                                                                                                                                              
    failed_attempts_last_1hr: int
    avg_transaction_amount_30d: float


@app.post("/predict")                                                                                                                                                               
def predict(transaction: Transaction):                                                                                                                                              
    df = pd.DataFrame([transaction.dict()])                                                                                                                                         
                
    df = pd.get_dummies(df, columns=["merchant_category", "device_type"])                                                                                                           

    df["location_city"] = le.transform(df["location_city"])                                                                                                                         
                
    model_features = model.get_booster().feature_names
    for col in model_features:
        if col not in df.columns:
            df[col] = 0

    df = df[model_features]

    prob = model.predict_proba(df)[0][1]                                                                                                                                            

    if prob < 0.3:                                                                                                                                                                  
        decision = "approve"
    elif prob < 0.7:
        decision = "review"
    else:
        decision = "block"

    return {"fraud_probability": round(float(prob), 4), "decision": decision}


