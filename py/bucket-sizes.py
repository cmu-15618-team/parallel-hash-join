import numpy as np
import matplotlib.pyplot as plt

def analyze_data(filepath):
    with open(filepath, 'r') as file:
        data = file.readlines()
    data = [float(line.strip()) for line in data]

    mean = np.mean(data)
    variance = np.var(data)

    print(f"Mean: {mean}, Variance: {variance}")

    plt.figure(figsize=(12, 6))
    plt.subplot(1, 2, 1)
    plt.hist(data, bins=20, color='blue', alpha=0.7, density=True)
    plt.title("Histogram of Bucket Sizes")
    plt.xlabel("Normalized Bucket Sizes")
    plt.ylabel("Frequency")

    plt.subplot(1, 2, 2)
    plt.boxplot(data, vert=True, patch_artist=True, notch=True)
    plt.title("Boxplot of Bucket Size Distribution")
    plt.ylabel("Bucket Sizes")

    plt.show()


analyze_data("../bucket_sizes.csv")
