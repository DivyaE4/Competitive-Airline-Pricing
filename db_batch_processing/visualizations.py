"""
Visualization tools for airline pricing data
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

def get_price_trends():
    """Get price trends from the database"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cur.execute("""
            SELECT 
                origin, 
                destination, 
                window_start, 
                avg_price
            FROM price_trends
            ORDER BY origin, destination, window_start
        """)
        
        rows = cur.fetchall()
        return [dict(row) for row in rows]

    except Exception as e:
        print(f"Error getting price trends: {e}")
        return []
    finally:
        if conn:
            conn.close()

def get_best_airlines():
    """Get best airlines by route from the database"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cur.execute("""
            SELECT 
                origin, 
                destination, 
                airline,
                airline_name,
                avg_price,
                window_start
            FROM best_airlines
            WHERE price_rank = 1
            ORDER BY origin, destination, window_start
        """)

        rows = cur.fetchall()
        return [dict(row) for row in rows]

    except Exception as e:
        print(f"Error getting best airlines: {e}")
        return []
    finally:
        if conn:
            conn.close()

def plot_price_trends(data):
    """Plot price trends by route"""
    if not data:
        print("No price trend data to plot")
        return
    
    df = pd.DataFrame(data)
    os.makedirs("figures", exist_ok=True)

    # Ensure datetime type
    if df['window_start'].dtype != 'datetime64[ns]':
        df['window_start'] = pd.to_datetime(df['window_start'])

    plt.figure(figsize=(12, 8))
    routes = df.groupby(['origin', 'destination'])

    for name, group in routes:
        route_name = f"{name[0]}-{name[1]}"
        plt.plot(group['window_start'], group['avg_price'], marker='o', linestyle='-', label=route_name)

    plt.xlabel('Time Window')
    plt.ylabel('Average Price (USD)')
    plt.title('Price Trends by Route')
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()

    filename = f"figures/price_trends_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    plt.savefig(filename)
    print(f"Saved price trends to {filename}")

def plot_best_airlines(data):
    """Plot best airlines by route"""
    if not data:
        print("No best airlines data to plot")
        return
    
    df = pd.DataFrame(data)
    os.makedirs("figures", exist_ok=True)

    df_latest = df.sort_values('window_start').groupby(['origin', 'destination', 'airline']).last().reset_index()

    plt.figure(figsize=(12, 8))
    route_names = [f"{row['origin']}-{row['destination']}" for _, row in df_latest.iterrows()]
    bars = plt.bar(route_names, df_latest['avg_price'], color='skyblue')

    for i, bar in enumerate(bars):
        airline = df_latest.iloc[i]['airline']
        plt.text(bar.get_x() + bar.get_width()/2, 5, airline, ha='center', va='bottom', rotation=0)

    plt.xlabel('Route')
    plt.ylabel('Average Price (USD)')
    plt.title('Best Airlines by Route (Lowest Average Price)')
    plt.xticks(rotation=45)
    plt.tight_layout()

    filename = f"figures/best_airlines_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    plt.savefig(filename)
    print(f"Saved best airlines to {filename}")

if __name__ == "__main__":
    print("Generating visualizations...")

    price_trend_data = get_price_trends()
    if price_trend_data:
        print(f"Found {len(price_trend_data)} price trend data points")
        plot_price_trends(price_trend_data)
    else:
        print("No price trend data found")

    best_airlines_data = get_best_airlines()
    if best_airlines_data:
        print(f"Found {len(best_airlines_data)} best airlines data points")
        plot_best_airlines(best_airlines_data)
    else:
        print("No best airlines data found")

