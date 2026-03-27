import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report, roc_auc_score
from xgboost import XGBClassifier
import mlflow
import mlflow.xgboost
import joblib




df = pd.read_csv("finsecure_transactions.csv")

df = df.drop(columns=["transaction_id", "user_id"])                                                                                                                                 
                                                                                                                                                                                      
df = pd.get_dummies(df, columns=["merchant_category", "device_type"])                                                                                                               

le = LabelEncoder()                                                                                                                                                                 
df["location_city"] = le.fit_transform(df["location_city"])


X = df.drop(columns=["is_fraud"])                                                                                                                                                   
y = df["is_fraud"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)


scale = y_train.value_counts()[0] / y_train.value_counts()[1]                                                                                                                       
                                                                                                                                                                                      
model = XGBClassifier(                                                                                                                                                              
    n_estimators=200,                                                                                                                                                               
    max_depth=6,
    learning_rate=0.05,
    scale_pos_weight=scale,
    random_state=42                                                                                                                                                                 
)
                                                                                                                                                                                    
model.fit(X_train, y_train)

y_pred = model.predict(X_test)                                                                                                                                                      
y_prob = model.predict_proba(X_test)[:, 1]

print(classification_report(y_test, y_pred))
print("AUC-ROC:", roc_auc_score(y_test, y_prob))

joblib.dump(model, "fraud_model.pkl")
joblib.dump(le, "label_encoder.pkl")

print("Model saved successfully.")

mlflow.set_tracking_uri("sqlite:///mlflow.db")
mlflow.set_experiment("finsecure_fraud_detection")
                                                                                                                                                                                      
with mlflow.start_run():
    mlflow.log_param("n_estimators", 200)
    mlflow.log_param("max_depth", 6)                                                                                                                                                
    mlflow.log_param("learning_rate", 0.05)
    mlflow.log_param("scale_pos_weight", scale)                                                                                                                                     
                
    mlflow.log_metric("auc_roc", roc_auc_score(y_test, y_prob))

    mlflow.xgboost.log_model(model, "model")                                                                                                                                        

    print("MLflow run logged successfully.")