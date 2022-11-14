import urllib.request
import pandas as pd
import numpy as np

DATA_URL = "https://archive.ics.uci.edu/ml/machine-learning-databases/00291/airfoil_self_noise.dat"
TRAIN_SIZE = 0.8
np.random.seed(42)

def main() -> None:
    with urllib.request.urlopen(DATA_URL) as f:
        headers = ["Frequency", "Angle_of_attack", "Chord_length", "Free_stream_velocity", "Suction_side_displacement_thickness", "Scaled_SPL"]
        d = pd.read_table(f, names=headers)
    
    df = pd.DataFrame(d)
    split_rule = np.random.rand(len(df)) < TRAIN_SIZE
    train_df = df[split_rule]
    test_df = df[~split_rule]
    train_df.to_csv("train.csv", index=False)
    test_df.to_csv("test.csv", index=False)
    
if __name__ == "__main__":
    main()