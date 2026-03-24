import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

def main():
    print("Initializing Stock ETL Pipeline...")
    
    # 1. Define the parameters
    # Feel free to add or change ticker symbols here
    tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NFLX', 'TSLA']
    
    # Set date range (Last 5 years)
    end_date = datetime.today()
    start_date = end_date - timedelta(days=5*365)
    
    all_price_data = []
    company_info_data = []

    # 2. Extract and Transform Data
    for ticker in tickers:
        print(f"Fetching data for {ticker}...")
        try:
            stock = yf.Ticker(ticker)
            
            # Fetch historical price data
            df = stock.history(start=start_date, end=end_date)
            
            if df.empty:
                print(f"Warning: No data found for {ticker}.")
                continue
                
            df.reset_index(inplace=True)
            
            # Standardize dates (remove timezone formatting which causes issues in SQL/Tableau)
            df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None)
            df['Date'] = df['Date'].dt.date # Keep only the date part
            
            # Add Ticker column
            df['Ticker'] = ticker
            
            # FEATURE ENGINEERING
            # Calculate 50-day and 200-day Simple Moving Averages
            df['50_Day_SMA'] = df['Close'].rolling(window=50).mean()
            df['200_Day_SMA'] = df['Close'].rolling(window=200).mean()
            
            # Calculate Daily Return Percentage
            df['Daily_Return_%'] = df['Close'].pct_change() * 100
            
            all_price_data.append(df)
            
            # Fetch Company Metadata
            info = stock.info
            company_info_data.append({
                'Ticker': ticker,
                'Company_Name': info.get('shortName', ticker),
                'Sector': info.get('sector', 'Unknown'),
                'Industry': info.get('industry', 'Unknown'),
                'Market_Cap': info.get('marketCap', 0)
            })
            
        except Exception as e:
            print(f"Error fetching {ticker}: {e}")

    # 3. Combine Data
    print("\nConsolidating datasets...")
    final_price_df = pd.concat(all_price_data, ignore_index=True)
    final_company_df = pd.DataFrame(company_info_data)

    # Select and reorder columns, dropping unnecessary ones like Dividends/Stock Splits
    final_price_df = final_price_df[['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume', '50_Day_SMA', '200_Day_SMA', 'Daily_Return_%']]
    
    # Round numerical columns to 2 decimal places for cleaner visualization
    cols_to_round = ['Open', 'High', 'Low', 'Close', '50_Day_SMA', '200_Day_SMA', 'Daily_Return_%']
    final_price_df[cols_to_round] = final_price_df[cols_to_round].round(2)

    # 4. Export (Load)
    # Saving to CSV. (If you want to push directly to MySQL, you would use sqlalchemy here)
    price_filename = 'stock_prices.csv'
    company_filename = 'company_info.csv'
    
    final_price_df.to_csv(price_filename, index=False)
    final_company_df.to_csv(company_filename, index=False)
    
    print(f"\n✅ ETL Complete! Data saved to:")
    print(f" - {price_filename} ({len(final_price_df)} rows)")
    print(f" - {company_filename} ({len(final_company_df)} rows)")

if __name__ == "__main__":
    main()