import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
import pickle
import os

def generate_training_data(n_samples=1000):
    print("📚 Generating training data...")
    np.random.seed(42)

    data = {
        "amount":           np.random.uniform(100, 2000, n_samples),
        "hour":             np.random.randint(0, 24, n_samples),
        "processing_time":  np.random.uniform(1, 10, n_samples),
        "item_count":       np.random.randint(1, 5, n_samples),
        "has_all_fields":   np.ones(n_samples),
    }

    df = pd.DataFrame(data)
    print(f"✅ Generated {n_samples} normal training samples")
    return df

def train_model():
    print("\n🧠 Training Isolation Forest model...")
    df    = generate_training_data()
    model = IsolationForest(
        contamination=0.05,
        random_state=42,
        n_estimators=100
    )
    model.fit(df)
    print("✅ Model trained successfully!")

    os.makedirs("anomaly_detector/models", exist_ok=True)
    with open("anomaly_detector/models/isolation_forest.pkl", "wb") as f:
        pickle.dump(model, f)

    print("💾 Model saved!")
    return model

if __name__ == "__main__":
    train_model()
    print("\n🎉 Training complete!")