import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from sklearn.decomposition import PCA
import glob
import os

#Read the csv from the spark query
def read_spark_csv(path):
    files = glob.glob(path + "/part-*.csv")
    if not files:
        print(f"No files found: {path}")
        return None
    return pd.read_csv(files[0])

# Folders
RESULTS_DIR = "../results"
OUTPUT_DIR  = "./output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ================================
# QUERY 1   
# ================================
def graph_query1():
    df = read_spark_csv(f"{RESULTS_DIR}/query1")
    if df is None:
        return

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle("Query 1 — AA vs DL", fontsize=14, fontweight='bold')

    colors = {'AA': 'blue', 'DL': 'orange'}

    # Graph 1
    ax = axes[0]
    for carrier in ['AA', 'DL']:
        data = df[df['OP_UNIQUE_CARRIER'] == carrier].sort_values('MONTH')
        ax.plot(data['MONTH'], data['dep_delay_mean'],
                marker='o', label=carrier, color=colors[carrier])
    ax.set_title('Monthly average departure delay')
    ax.set_xlabel('Month')
    ax.set_ylabel('Minutes')
    ax.set_xticks(range(1, 5))
    ax.legend()
    ax.grid(True)

    # Graph 2
    ax = axes[1]
    for carrier in ['AA', 'DL']:
        data = df[df['OP_UNIQUE_CARRIER'] == carrier].sort_values('MONTH')
        ax.plot(data['MONTH'], data['cancellation_rate'],
                marker='o', label=carrier, color=colors[carrier])
    ax.set_title("Monthly cancellation rate (%)")
    ax.set_xlabel('Month')
    ax.set_ylabel('%')
    ax.set_xticks(range(1, 5))
    ax.legend()
    ax.grid(True)

    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/query1_graphs.png", dpi=150)
    print("Query1 graph done")
    plt.close()

# ================================
# QUERY 2
# ================================
def graph_query2():
    df = read_spark_csv(f"{RESULTS_DIR}/query2")
    if df is None:
        return

    fig, axes = plt.subplots(1, 2, figsize=(16, 7))
    fig.suptitle("Query 2 — Top 10 Airlines", fontsize=14, fontweight='bold')

    # Graph 1
    ax = axes[0]
    df_sorted = df.sort_values('avg_arr_delay', ascending=True)
    ax.barh(df_sorted['OP_UNIQUE_CARRIER'], df_sorted['avg_arr_delay'],
            color='steelblue', alpha=0.8)
    ax.set_title("Top 10 airlines by average arrival delay")
    ax.set_xlabel('Minutes')
    ax.set_ylabel('Airlines')
    ax.grid(axis='x')

    # Graph 2
    ax = axes[1]
    delay_cols = [
        'avg_carrier_delay', 'avg_weather_delay',
        'avg_nas_delay', 'avg_security_delay',
        'avg_late_aircraft_delay'
    ]
    labels = ['Carrier', 'Weather', 'NAS', 'Security', 'Late Aircraft']

    df_plot = df.set_index('OP_UNIQUE_CARRIER')[delay_cols]
    df_plot.columns = labels
    df_plot.plot(kind='bar', stacked=True, ax=ax, colormap='Set2')
    ax.set_title('Average impact of the different delay causes')
    ax.set_xlabel('Airlines')
    ax.set_ylabel('Minutes')
    ax.tick_params(axis='x', rotation=45)
    ax.legend(loc='upper right')
    ax.grid(axis='y')

    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/query2_graphs.png", dpi=150)
    print("Query2 graph done")
    plt.close()
# ================================
# QUERY 3 
# ================================
def graph_query3():
    df = read_spark_csv(f"{RESULTS_DIR}/query3/percentile")
    if df is None:
        return

    carriers = ['AA', 'DL', 'UA', 'WN']
    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.suptitle("Query 3 — Distribution across time for each companies",
                 fontsize=14, fontweight='bold')

    for idx, carrier in enumerate(carriers):
        ax   = axes[idx // 2][idx % 2]
        data = df[df['OP_UNIQUE_CARRIER'] == carrier].sort_values('hour')

        ax.plot(data['hour'], data['p25'], label='P25', linestyle='--', alpha=0.7)
        ax.plot(data['hour'], data['p50'], label='P50', linewidth=2)
        ax.plot(data['hour'], data['p75'], label='P75', linestyle='--', alpha=0.7)
        ax.plot(data['hour'], data['p90'], label='P90', linestyle=':', alpha=0.7)
        ax.fill_between(data['hour'], data['p25'], data['p75'], alpha=0.1)

        ax.set_title(f'{carrier}')
        ax.set_xlabel('Hour')
        ax.set_ylabel('Delay (minutes)')
        ax.set_xticks(range(0, 24))
        ax.legend()
        ax.grid(True)

    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/query3_graphs.png", dpi=150)
    print("Query3 graph done")
    plt.close()
# ================================
# QUERY 4 
# ================================
def graph_query4():
    df = read_spark_csv(f"{RESULTS_DIR}/query4")
    if df is None:
        return

    features = [
        'avg_arr_delay', 'avg_dep_delay', 'cancellation_rate',
        'avg_carrier_delay', 'avg_weather_delay', 'avg_nas_delay',
        'avg_security_delay', 'avg_late_aircraft_delay'
    ]

    fig, axes = plt.subplots(1, 2, figsize=(16, 7))
    fig.suptitle("Query 4 — Clustering K-Means ",
                 fontsize=14, fontweight='bold')

    # Graph 1  PCA 2D
    ax = axes[0]
    pca    = PCA(n_components=2)
    coords = pca.fit_transform(df[features].fillna(0))
    colors = cm.Set1(df['cluster'] / df['cluster'].max())

    scatter = ax.scatter(coords[:, 0], coords[:, 1],
                        c=df['cluster'], cmap='Set1', s=150)
    for i, carrier in enumerate(df['OP_UNIQUE_CARRIER']):
        ax.annotate(carrier, (coords[i, 0], coords[i, 1]),
                   textcoords="offset points", xytext=(5, 5), fontsize=9)

    ax.set_title('PCA cluster')
    ax.set_xlabel(f"PCA 1 ({pca.explained_variance_ratio_[0]*100:.1f}%)")
    ax.set_ylabel(f"PCA 2 ({pca.explained_variance_ratio_[1]*100:.1f}%)")
    plt.colorbar(scatter, ax=ax, label='Cluster')
    ax.grid(True)

    # Graph 2
    ax = axes[1]
    cluster_means = df.groupby('cluster')[features].mean()
    cluster_means.T.plot(kind='bar', ax=ax, colormap='Set1', alpha=0.8)
    ax.set_title('Profil mby cluster')
    ax.set_xlabel('Feature')
    ax.set_ylabel('Average value')
    ax.tick_params(axis='x', rotation=45)
    ax.legend([f'Cluster {i}' for i in cluster_means.index])
    ax.grid(axis='y')

    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/query4_graphs.png", dpi=150)
    print("Query4 graph done")
    plt.close()

# ================================
# MAIN
# ================================
if __name__ == "__main__":
    print("=== Generation of the graphs ===")
    graph_query1()
    graph_query2()
    graph_query3()
    graph_query4()
    print(f"=== Graphs store in {OUTPUT_DIR}/ ===")