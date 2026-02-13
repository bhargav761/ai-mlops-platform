from sklearn.ensemble import IsolationForest
import numpy as np
import joblib
import os

# Fake normal training data
X = np.random.normal(loc=50, scale=5, size=(500, 1))

model = IsolationForest(contamination=0.05)
model.fit(X)

joblib.dump(model, "model.pkl")
print("Model trained and saved as model.pkl")
