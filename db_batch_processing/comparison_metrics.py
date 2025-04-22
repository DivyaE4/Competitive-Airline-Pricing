"""
Performance evaluation logic for comparing streaming vs batch mode
"""
import psycopg2
import psycopg2.extras
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime

# PostgreSQL connection parameters
DB_PARAMS = {
    "dbname": "airlinepricing",
    "user": "airlineuser",
    "password": "airlinepass",
    "host": "localhost",
    "port": "5432"
}

def get_comparison_metrics():
    """Retrieve comparison metrics from the database"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        cur.execute("""
            SELECT 
                query_type,
                streaming_execution_time,
                batch_execution_time,
                streaming_result_count,
                batch_result_count,
                streaming_avg_price,
                batch_avg_price,
                price_diff_percentage,
                comparison_timestamp
            FROM comparison_metrics
            ORDER BY comparison_timestamp DESC
            LIMIT 10
        """)
        
        rows = cur.fetchall()
        return [dict(row) for row in rows]  # ✅ Convert to dicts

    except Exception as e:
        print(f"Error getting comparison metrics: {e}")
        return []
    finally:
        if conn:
            conn.close()

def plot_execution_time_comparison(data):
    """Plot execution time comparison between streaming and batch"""
    if not data:
        print("No data to plot")
        return
    
    df = pd.DataFrame(data)
    os.makedirs("figures", exist_ok=True)

    # Group by query_type and calculate mean execution time
    streaming_times = df.groupby('query_type')['streaming_execution_time'].mean()
    batch_times = df.groupby('query_type')['batch_execution_time'].mean()

    # Plot execution time comparison
    plt.figure(figsize=(10, 6))
    bar_width = 0.35
    index = range(len(streaming_times))

    plt.bar(index, streaming_times, bar_width, label='Streaming')
    plt.bar([i + bar_width for i in index], batch_times, bar_width, label='Batch')

    plt.xlabel('Query Type')
    plt.ylabel('Execution Time (seconds)')
    plt.title('Streaming vs Batch Execution Time')
    plt.xticks([i + bar_width / 2 for i in index], streaming_times.index)
    plt.legend()

    filename = f"figures/execution_time_comparison_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    plt.savefig(filename)
    print(f"Saved execution time comparison to {filename}")

    # Plot price difference percentage
    plt.figure(figsize=(10, 6))
    price_diff = df.groupby('query_type')['price_diff_percentage'].mean()
    plt.bar(price_diff.index, price_diff, color='green')

    plt.xlabel('Query Type')
    plt.ylabel('Price Difference (%)')
    plt.title('Price Difference Between Streaming and Batch Results')

    filename = f"figures/price_difference_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    plt.savefig(filename)
    print(f"Saved price difference comparison to {filename}")

    # Summary
    print("\nExecution Time Summary (seconds):")
    execution_time_summary = pd.DataFrame({
        'Streaming': streaming_times,
        'Batch': batch_times,
        'Speedup': batch_times / streaming_times
    })
    print(execution_time_summary)

    print("\nPrice Difference Summary (%):")
    print(price_diff)

    return execution_time_summary, price_diff

if __name__ == "__main__":
    print("Analyzing comparison metrics...")
    data = get_comparison_metrics()
    
    if data:
        print(f"Found {len(data)} comparison metrics")
        execution_time_summary, price_diff = plot_execution_time_comparison(data)
    else:
        print("No comparison metrics found")

