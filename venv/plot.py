import pandas as pd
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt


def plot_speed_test(test_file_name, tested_data_name):
    df = pd.read_csv(f"{test_file_name}.csv", header=None, names=['Rows', 'Time', 'Version'])

    df['Rows'] = df['Rows'].astype(int)
    df['Time'] = df['Time'].astype(float)

    plt.figure(figsize=(12, 6))
    sns.scatterplot(data=df, x='Rows', y='Time', hue='Version', style='Version', s=100)

    plt.xscale('log')
    plt.yscale('log')

    plt.title(f'Speed Comparison - {tested_data_name}')
    plt.xlabel('Number of Rows (log scale)')
    plt.ylabel('Time (seconds - log scale)')
    plt.legend(title='Version')

    plt.show()


def plot_column_speed_test(test_file_name, tested_data_name):
    df = pd.read_csv(f"{test_file_name}.csv", header=None, names=['Columns', 'Version', 'Time'])

    df['Columns'] = df['Columns'].astype(int)
    df['Time'] = df['Time'].astype(float)

    plt.figure(figsize=(12, 6))
    sns.scatterplot(data=df, x='Columns', y='Time', hue='Version', style='Version', s=100)

    #plt.yscale('log')

    plt.title(f'Speed Comparison - {tested_data_name}')
    plt.xlabel('Number of Columns')
    plt.ylabel('Time')
    plt.legend(title='Version')

    plt.show()


def plot_memory_usage(test_file_name, tested_data_name):
    df = pd.read_csv(f"{test_file_name}.csv", header=None, names=['Rows', 'MB', 'Version'])

    df['Rows'] = df['Rows'].astype(int)
    df['MB'] = df['MB'].astype(float)

    row_values = df['Rows'].unique()
    x = np.arange(len(row_values) * 2, step=2)  # Abstände zwischen den Gruppen schaffen
    width = 0.3

    fig, ax = plt.subplots(figsize=(14, 8))

    spatial_times = df[df['Version'] == 'Spatial']['MB'].values
    ax.bar(x - width, spatial_times, width * 2, label='Spatial')

    versions = df['Version'].unique()
    versions = [v for v in versions if v != 'Spatial']

    for i, version in enumerate(versions):
        sheetreader_times = df[df['Version'] == version]['MB'].values
        ax.bar(x + (i * width), sheetreader_times, width, label=version)

    ax.set_xlabel('Rows')
    ax.set_ylabel('Peak Memory Usage (MB)')
    ax.set_title(f'Memory Usage Comparison: {tested_data_name}')
    ax.set_xticks(x)
    ax.set_xticklabels(row_values)
    #ax.set_yscale('log')
    ax.legend()

    plt.show()


def plot_column_memory_usage(test_file_name, tested_data_name):
    df = pd.read_csv(f"{test_file_name}.csv", header=None, names=['Columns', 'Version', 'MB'])

    df['Columns'] = df['Columns'].astype(int)
    df['MB'] = df['MB'].astype(float)

    column_values = sorted(df['Columns'].unique())  # Werte sortieren, um bei 1 zu beginnen
    x = np.arange(len(column_values) * 2, step=2)  # Abstände zwischen den Gruppen schaffen
    width = 0.3

    fig, ax = plt.subplots(figsize=(14, 8))

    spatial_times = [df[(df['Columns'] == col) & (df['Version'] == 'Spatial')]['MB'].values[0] for col in column_values]
    ax.bar(x - width, spatial_times, width * 2, label='Spatial')

    versions = df['Version'].unique()
    versions = [v for v in versions if v != 'Spatial']

    for i, version in enumerate(versions):
        sheetreader_times = [df[(df['Columns'] == col) & (df['Version'] == version)]['MB'].values[0] for col in column_values]
        ax.bar(x + (i * width), sheetreader_times, width, label=version)

    ax.set_xlabel('Columns')
    ax.set_ylabel('Peak Memory Usage (MB)')
    ax.set_title(f'Memory Usage Comparison: {tested_data_name}')
    ax.set_xticks(x)
    ax.set_xticklabels(column_values)
    ax.legend()
    ax.set_yscale('log')

    plt.show()



#plot_speed_test("./string_output", "TPC-H Customer Speed Test - Pure String")
#plot_flag_test("./test_flag_output", "./flag_output.csv", "Flag Test")
#plot_column_speed_test("./new_tpch_results", "1 million Rows - Scaled Columns")
#plot_memory_usage("/Users/finnklapper/PycharmProjects/dbpro/dbpro_benchmarking/range_memory_output", "Peak Memory Consumption")
#plot_column_memory_usage("/Users/finnklapper/PycharmProjects/dbpro/dbpro_benchmarking/columns_memory_output", "Column Scaling Peak Memory Consumption")