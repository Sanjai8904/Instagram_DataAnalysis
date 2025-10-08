import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import os

# -------- Config ----------
CSV_PATH = "insta_dataset.csv"   # change if needed
CLEANED_CSV = "cleaned_insta_dataset.csv"

# MySQL Connection Details (update with your credentials)
MYSQL_USER = "root"
MYSQL_PASSWORD = "****"
MYSQL_HOST = "localhost"
MYSQL_DB = "instagram_analytics"
# --------------------------

def load_data(path):
    df = pd.read_csv(path)
    print("Initial shape:", df.shape)
    return df

def clean_and_engineer(df):
    df = df.dropna(how="all")  # drop empty rows
    df = df.drop_duplicates()  # drop duplicates

    # Ensure numeric columns are integers
    for col in ["likes", "comments", "shares", "views", "followers_count"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # Parse date
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date'])

    # Fill missing values
    df['caption'] = df['caption'].fillna("")
    df['hashtags'] = df['hashtags'].fillna("")

    # Feature engineering
    df['day_of_week'] = df['date'].dt.day_name()
    df['hour'] = df['date'].dt.hour
    df['caption_word_count'] = df['caption'].apply(lambda x: len(str(x).split()))
    df['engagement'] = df['likes'] + df['comments'] + df['shares']

    # Avoid division by zero in engagement rate
    df['followers_count'] = df['followers_count'].replace(0, np.nan).fillna(method='ffill').fillna(1).astype(int)
    df['engagement_rate'] = df['engagement'] / df['followers_count']

    # Normalize hashtags
    df['hashtags'] = df['hashtags'].astype(str).str.lower().str.strip()

    return df

def top_posts(df, n=10):
    return df.sort_values('engagement', ascending=False).head(n)

def engagement_by_day(df):
    return df.groupby('day_of_week')['engagement'].mean().reindex(
        ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    )

def engagement_by_hour(df):
    return df.groupby('hour')['engagement'].mean().sort_index()

def engagement_by_content_type(df):
    return df.groupby('content_type')['engagement'].mean().sort_values(ascending=False)

def top_hashtags(df, n=10):
    s = df['hashtags'].fillna("").astype(str)
    tags = s.str.replace(",", " ").str.split().explode()
    tags = tags[tags != ""]
    return tags.value_counts().head(n)

def plot_series(series, title, xlabel=None, ylabel=None, filename=None):
    plt.figure(figsize=(8,4))
    series.plot(kind='bar' if series.index.dtype == object else 'line')
    plt.title(title)
    if xlabel: plt.xlabel(xlabel)
    if ylabel: plt.ylabel(ylabel)
    plt.tight_layout()
    if filename:
        plt.savefig(filename)
        print("Saved plot:", filename)
    plt.show()

def save_to_mysql(df):
    try:
        engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}")
        df.to_sql("posts", engine, if_exists="replace", index=False)
        print(f"✅ Saved cleaned data to MySQL database `{MYSQL_DB}`, table `posts`")
    except Exception as e:
        print("❌ Error saving to MySQL:", e)

def main():
    if not os.path.exists(CSV_PATH):
        print("CSV not found:", CSV_PATH)
        return

    df = load_data(CSV_PATH)
    df_clean = clean_and_engineer(df)

    # Save cleaned CSV
    df_clean.to_csv(CLEANED_CSV, index=False)
    print("✅ Saved cleaned CSV:", CLEANED_CSV)

    # Quick analysis
    print("\nTop 5 posts by engagement:")
    print(top_posts(df_clean, 5)[["post_id","date","content_type","likes","comments","shares","engagement"]])

    print("\nAverage engagement by day of week:")
    print(engagement_by_day(df_clean))

    print("\nAverage engagement by hour:")
    print(engagement_by_hour(df_clean).head(24))

    print("\nAverage engagement by content type:")
    print(engagement_by_content_type(df_clean))

    print("\nTop hashtags:")
    print(top_hashtags(df_clean))

    # Plots
    plot_series(engagement_by_day(df_clean), "Average Engagement by Day of Week", xlabel="Day", ylabel="Avg Engagement", filename="engagement_by_day.png")
    plot_series(engagement_by_hour(df_clean), "Average Engagement by Hour of Day", xlabel="Hour", ylabel="Avg Engagement", filename="engagement_by_hour.png")
    plot_series(engagement_by_content_type(df_clean), "Average Engagement by Content Type", xlabel="Content Type", ylabel="Avg Engagement", filename="engagement_by_content.png")

    fg = df_clean.groupby('date')['followers_count'].max().sort_index()
    plot_series(fg, "Follower Count Over Time", xlabel="Date", ylabel="Followers", filename="followers_over_time.png")

    # Save to MySQL
    save_to_mysql(df_clean)

if __name__ == "__main__":
    main()
