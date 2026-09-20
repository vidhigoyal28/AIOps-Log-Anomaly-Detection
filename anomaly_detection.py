import pandas as pd
import matplotlib.pyplot as plt

# 1. Create sample dataset
data = {
    "Timestamp": [
        "10:00", "10:01", "10:02", "10:03", "10:04",
        "10:05", "10:06", "10:07", "10:08", "10:09",
        "10:10", "10:11", "10:12", "10:13", "10:14",
        "10:15", "10:16", "10:17", "10:18", "10:19"
    ],

    "CPU": [
        45, 50, 52, 48, 55,
        95, 60, 58, 62, 65,
        67, 70, 97, 72, 68,
        75, 80, 85, 92, 78
    ],

    "Memory": [
        50, 52, 51, 53, 55,
        60, 62, 61, 64, 65,
        67, 68, 70, 71, 72,
        74, 76, 78, 80, 79
    ],

    "Response_Time": [
        200, 210, 220, 205, 230,
        250, 260, 270, 280, 290,
        300, 310, 320, 330, 340,
        350, 360, 370, 380, 390
    ]
}

df = pd.DataFrame(data)

# 2. Basic statistics
print("Total records:", len(df))

print("\nBasic Statistics:")
print(df[["CPU", "Memory", "Response_Time"]].describe())

# 3. Detect anomalies using threshold
df["Anomaly"] = (
    (df["CPU"] > 90) |
    (df["Memory"] > 90) |
    (df["Response_Time"] > 500)
)

# 4. Print anomalous records
anomalies = df[df["Anomaly"]]

print("\nAnomalies detected:", len(anomalies))

print("\nAnomalous Records:")
print(anomalies[["Timestamp", "CPU", "Memory", "Response_Time"]])

# 5. Plot CPU values and anomalies
plt.figure(figsize=(10, 5))

plt.plot(
    df["Timestamp"],
    df["CPU"],
    marker="o",
    label="CPU Usage"
)

plt.scatter(
    anomalies["Timestamp"],
    anomalies["CPU"],
    label="Anomaly"
)

plt.axhline(
    y=90,
    linestyle="--",
    label="CPU Threshold"
)

plt.xlabel("Timestamp")
plt.ylabel("CPU Usage (%)")
plt.title("CPU Usage and Anomalies")
plt.xticks(rotation=45)
plt.legend()
plt.tight_layout()

plt.show()