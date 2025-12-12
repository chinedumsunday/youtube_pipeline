import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import logging

# 1. Page Config: Set wide mode and a custom page icon
st.set_page_config(
    page_title="🎬 YouTube Video Data Analytics (NG region primarily)", 
    layout="wide",
    page_icon="🎬"
)

# 2. Custom CSS to remove top padding and bland styling
st.markdown("""
    <style>
        .block-container {padding-top: 1rem; padding-bottom: 0rem;}
        h1 {margin-bottom: 0rem;}
        div[data-testid="stMetric"] {
            background-color: #262730;
            border: 1px solid #464b5f;
            padding: 15px;
            border-radius: 10px;
            color: white;
        }
    </style>
""", unsafe_allow_html=True)

# Helper function to cache data (speeds up the app)
@st.cache_data
def load_data(path):
    try:
        return pd.read_csv(path)
    except Exception as e:
        return None

# Helper to format arrows
def format_arrow(val):
    if val > 0:
        return "🟢 ▲"
    elif val < 0:
        return "🔴 ▼"
    else:
        return "➖"

st.title("🎬 YouTube Video Data Analytics (NG region primarily)")
st.markdown("Daily performance metrics and content insights.")
st.markdown("---")

# --- ROW 1: TOP VIDEOS & CHANNEL INSIGHTS ---
col1, col2 = st.columns([1.5, 1], gap="large")

with col1:
    st.subheader("🔥 Top 10 Videos")
    df_top = load_data('./results/top_videos_by_views.csv')
    
    if df_top is not None:
        # Use Plotly for interactive chart
        fig = px.bar(
            df_top.head(10), 
            x='view_count', 
            y='title', 
            orientation='h',
            text_auto='.2s', # Makes numbers readable (e.g. 1.2M)
            color='view_count',
            color_continuous_scale='Bluered_r'
        )
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)", 
            xaxis=(dict(showgrid=False)),
            margin=dict(l=0, r=0, t=0, b=0),
            height=350
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.error("Data missing")

with col2:
    st.subheader("📊 Channel Insights")
    df_channels = load_data('./results/channel_insights.csv')
    
    if df_channels is not None:
        # Configure the column to show a progress bar instead of just numbers
        st.dataframe(
            df_channels.head(10),
            column_config={
                "view_count": st.column_config.ProgressColumn(
                    "Total Views",
                    help="Volume of views per channel",
                    format="%d",
                    min_value=0,
                    max_value=int(df_channels['view_count'].max()),
                ),
            },
            hide_index=True,
            use_container_width=True,
            height=350
        )
    else:
        st.error("Data missing")

st.markdown("---")

# --- ROW 2: DAILY GROWTH & RANK MOVERS ---
col3, col4 = st.columns([2,1], gap="large")

with col3:
    st.subheader("📈 Daily Growth Analysis")
    df_growth = load_data('./results/daily_growth.csv')

    if df_growth is not None:
        # Sort and Slice Top 10
        df_growth_sorted = df_growth.sort_values(by="daily_view_growth", ascending=False)
        df_top_10 = df_growth_sorted.head(10)

        # 1. The Data Table (keeps it compact above the big graph)
        display_df = df_top_10.copy()
        display_df['Trend'] = display_df['daily_view_growth'].apply(format_arrow)
        
        st.dataframe(
            display_df[['Trend', 'title', 'daily_view_growth', 'fetched_date']],
            column_config={
                "daily_view_growth": st.column_config.NumberColumn("Growth", format="%d"),
                "title": st.column_config.TextColumn("Video Title", width="large"), # Wider text column
            },
            hide_index=True,
            use_container_width=True
        )

        # 2. The "Hero" Graph (Bigger Height & Width)
        fig_growth = px.line(
            df_top_10, 
            x='fetched_date', 
            y='daily_view_growth', 
            color='title',
            markers=True,
            title="Top 10 Fastest Growing Videos (Trend)"
        )
        
        # UI TWEAK: Increased height to 500px for better visibility
        fig_growth.update_layout(
            plot_bgcolor="rgba(0,0,0,0)", 
            xaxis=(dict(showgrid=False)),
            legend=dict(orientation="h", y=-0.2), 
            height=500,  # <--- MADE TALLER
            margin=dict(l=0, r=0, t=30, b=0)
        )
        st.plotly_chart(fig_growth, use_container_width=True)

        # 3. The Expander
        with st.expander("🔍 View Growth for ALL Videos"):
            st.dataframe(df_growth_sorted, use_container_width=True)
            
            # # Full Table with search enabled
            # st.dataframe(
            #     df_growth_sorted,
            #     column_config={
            #         "daily_view_growth": st.column_config.NumberColumn("Growth", format="%d"),
            #     },
            #     use_container_width=True,
            #     height=500
            # )
            
            # Full Chart (Warning: Might be messy if many videos, but user asked for it!)
            fig_full = px.line(
                df_growth_sorted, 
                x='fetched_date', 
                y='daily_view_growth', 
                color='title'
            )
            st.plotly_chart(fig_full, use_container_width=True)

    else:
        st.error("Growth data could not be loaded.")

with col4:
    st.subheader("📉 Daily Rank Movers")
    df_rank = load_data('./results/daily_rank_movers.csv')

    if df_rank is not None:
        # Compact Sparkline Table
        st.dataframe(
            df_rank.head(10),
            column_config={
                "daily_rank_change": st.column_config.LineChartColumn(
                    "Rank Trend",
                    y_min=-10, y_max=10
                ),
                "title": st.column_config.TextColumn("Video", width="small"), # Smaller text to fit
            },
            hide_index=True,
            use_container_width=True
        )

        # Compact Bubble Chart
        fig_rank = px.scatter(
            df_rank, 
            x='fetched_date', 
            y='daily_rank_change', 
            color='title',
            size='daily_rank_change', 
            size_max=15
        )
        fig_rank.update_layout(
            plot_bgcolor="rgba(0,0,0,0)", 
            xaxis=(dict(showgrid=False)),
            showlegend=False, # Hide legend to save space in narrow column
            height=400, # Slightly smaller than the main graph
            margin=dict(l=0, r=0, t=30, b=0)
        )
        st.plotly_chart(fig_rank, use_container_width=True)

# --- ROW 3: NEW ENTRIES (Visual Cards) ---
st.markdown("---")
st.subheader("🆕 New Entries Radar")

df_new = load_data('./results/new_entries.csv')

if df_new is not None and not df_new.empty:
    # Instead of a boring list, let's use Metrics or Tiles
    # We display the first 4 new entries as Cards
    
    new_cols = st.columns(4)
    for i, row in enumerate(df_new.head(4).itertuples()):
        with new_cols[i % 4]:
            st.metric(
                label=f"New Entry #{i+1}",
                value=row.title[:20] + "..." if len(row.title) > 20 else row.title, # Truncate long titles
                delta="Just Added"
            )
    
    # Show the full list below if needed in an expander
    with st.expander("See all new entries"):
        st.dataframe(df_new, use_container_width=True)
else:
    st.info("No new entries today.")