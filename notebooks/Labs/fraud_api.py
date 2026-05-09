from fastapi import FastAPI
from pydantic import BaseModel
import pickle, numpy as np

app = FastAPI(title="Fraud Detection API")
model = pickle.load(open('fraud_model.pkl', 'rb'))

class Transaction(BaseModel):
    amount: float
    is_electronics: int
    tx_per_minute: int

@app.get("/health")
def health():
    return {"status": "ok"}
 
 
@app.post("/score")
def score(tx: Transaction):
    X = np.array([[tx.amount, tx.is_electronics, tx.tx_per_minute]])
    proba = model.predict_proba(X)[0,1]
    return {"is_fraud": bool(proba >= 0.5), "fraud_probability": round(float(proba),4)}
