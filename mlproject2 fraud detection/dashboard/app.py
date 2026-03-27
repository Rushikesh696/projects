import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import requests

st.set_page_config(page_title="FinSecure Fraud Monitor", layout="wide")

st.title("FinSecure - Real Time Fraud Detection Dashboard")

df = pd.read_csv("predictions.csv")                                                                                                                                      

total = len(df)

fraud_count = len(df[df["decision"] == "block"])
review_count = len(df[df["decision"] == "review"])
fraud_rate = round((fraud_count / total) * 100, 2)


col1, col2, col3, col4 = st.columns(4)

col1.metric("Total Transactions", total)
col2.metric("Blocked (Fraud)", fraud_count)
col3.metric("Under Review", review_count)
col4.metric("Fraud Rate", f"{fraud_rate}%")


st.subheader("Fraud Probability Distribution")

fig, ax = plt.subplots(figsize=(6, 3))
ax.hist(df["fraud_probability"], bins=20)
ax.set_xlabel("Fraud Probability")
ax.set_ylabel("Number of Transactions")
st.pyplot(fig)


st.subheader("Recent Transactions")
st.dataframe(df)

st.subheader("Fraud by Merchant Category")                                                                                                                                          
                  
category_fraud = df[df["decision"] == "block"]["merchant_category"].value_counts()                                                                                  

fig2, ax2 = plt.subplots(figsize=(6, 3))                                                                                                                                                          
ax2.bar(category_fraud.index, category_fraud.values)
ax2.set_xlabel("Merchant Category")
ax2.set_ylabel("Fraud Count")
plt.xticks(rotation=45)                                                                                                                                                             
st.pyplot(fig2)

st.subheader("Fraud by Device Type")                                                                                                                                                
                
device_fraud = df.groupby("device_type")["fraud_probability"].mean()                                                                                                        

fig3, ax3 = plt.subplots(figsize=(4, 3))                                                                                                                                                          
ax3.bar(device_fraud.index, device_fraud.values)
ax3.set_xlabel("Device Type")
ax3.set_ylabel("Avg Fraud Probability")
st.pyplot(fig3)



