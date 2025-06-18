#!/usr/bin/env python3
"""
Gapper Statistics Data Collector
Fetches real gapper data from Polygon API and generates JSON for dashboard
"""

import pandas as pd
import numpy as np
import requests
import json
import os
from datetime import datetime, timedelta, time
import pytz
from typing import Dict, List, Tuple
import time as time_module

class GapperDataCollector:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.polygon.io"
        self.eastern = pytz.timezone('US/Eastern')
        
    def get_premarket_movers(self, date: str) -> List[Dict]:
        """Get stocks with significant pre-market gaps"""
        # Get previous trading day close
        prev_date = self.get_previous_trading_day(date)
        
        # Get gainers/losers snapshot
        url = f"{self.base_url}/v2/snapshot/locale/us/markets/stocks/gainers"
        params = {
            'apiKey': self.api_key
        }
        
        try:
            response = requests.get(url, params=params)
            data = response.json()
            
            # Filter for 40%+ gaps with volume > 1M and price > $0.30
            gappers = []
            for ticker_data in data.get('tickers', []):
                gap_pct = ticker_data.get('todaysChangePerc', 0)
                volume = ticker_data.get('prevDay', {}).get('v', 0)
                price = ticker_data.get('day', {}).get('c', 0)
                
                if abs(gap_pct) >= 40 and volume >= 1000000 and price >= 0.30:
                    gappers.append({
                        'ticker': ticker_data['ticker'],
                        'gap_percentage': gap_pct,
                        'volume': volume,
                        'price': price
                    })
                    
            return gappers
            
        except Exception as e:
            print(f"Error fetching gappers: {e}")
            return []
    
    def get_intraday_data(self, ticker: str, date: str) -> pd.DataFrame:
        """Get 5-minute intraday data for a ticker"""
        url = f"{self.base_url}/v2/aggs/ticker/{ticker}/range/5/minute/{date}/{date}"
        params = {
            'apiKey': self.api_key,
            'adjusted': 'true',
            'sort': 'asc'
        }
        
        try:
            response = requests.get(url, params=params)
            data = response.json()
            
            if data.get('status') == 'OK' and data.get('results'):
                df = pd.DataFrame(data['results'])
                df['t'] = pd.to_datetime(df['t'], unit='ms', utc=True).dt.tz_convert(self.eastern)
                df['time'] = df['t'].dt.strftime('%H:%M')
                
                # Calculate price change from open
                open_price = df.iloc[0]['o']
                df['price_change_pct'] = ((df['c'] - open_price) / open_price) * 100
                
                return df[['time', 'o', 'h', 'l', 'c', 'v', 'price_change_pct']]
            
        except Exception as e:
            print(f"Error fetching intraday data for {ticker}: {e}")
            
        return pd.DataFrame()
    
    def calculate_monthly_statistics(self, month_data: List[Dict]) -> Dict:
        """Calculate statistics for a month of gapper data"""
        if not month_data:
            return {}
        
        # Aggregate all intraday movements
        all_movements = {}
        total_gappers = len(month_data)
        profitable_shorts = 0
        
        for gapper in month_data:
            intraday = gapper.get('intraday_data', [])
            if not intraday:
                continue
                
            # Check if profitable short (closed below open)
            if intraday[-1]['price_change_pct'] < 0:
                profitable_shorts += 1
            
            # Aggregate movements by time
            for point in intraday:
                time_key = point['time']
                if time_key not in all_movements:
                    all_movements[time_key] = []
                all_movements[time_key].append(point['price_change_pct'])
        
        # Calculate averages for each time point
        intraday_averages = []
        for time_key in sorted(all_movements.keys()):
            movements = all_movements[time_key]
            intraday_averages.append({
                'time': time_key,
                'avgPriceChange': np.mean(movements),
                'volume': len(movements) * 1000000  # Placeholder
            })
        
        # Find high and low points
        if intraday_averages:
            high_point = max(intraday_averages, key=lambda x: x['avgPriceChange'])
            low_point = min(intraday_averages, key=lambda x: x['avgPriceChange'])
            
            return {
                'totalGappers': total_gappers,
                'avgOpenToClose': intraday_averages[-1]['avgPriceChange'] if intraday_averages else 0,
                'medianHighTime': high_point['time'],
                'medianHighValue': high_point['avgPriceChange'],
                'medianLowValue': low_point['avgPriceChange'],
                'profitableShorts': (profitable_shorts / total_gappers * 100) if total_gappers > 0 else 0
            }
        
        return {}
    
    def collect_data(self, lookback_months: int = 12) -> Dict:
        """Collect all data for the dashboard"""
        end_date = datetime.now(self.eastern)
        monthly_data = []
        all_gappers = []
        
        # Collect data for each month
        for month_offset in range(lookback_months):
            month_start = end_date - timedelta(days=30 * (month_offset + 1))
            month_end = end_date - timedelta(days=30 * month_offset)
            
            print(f"Collecting data for {month_start.strftime('%B %Y')}...")
            
            month_gappers = []
            current_date = month_start
            
            while current_date <= month_end:
                if current_date.weekday() < 5:  # Weekday
                    date_str = current_date.strftime('%Y-%m-%d')
                    
                    # Get gappers for this day
                    daily_gappers = self.get_premarket_movers(date_str)
                    
                    for gapper in daily_gappers:
                        # Get intraday data
                        intraday_df = self.get_intraday_data(gapper['ticker'], date_str)
                        
                        if not intraday_df.empty:
                            gapper['date'] = date_str
                            gapper['intraday_data'] = intraday_df.to_dict('records')
                            month_gappers.append(gapper)
                            all_gappers.append(gapper)
                    
                    # Rate limiting
                    time_module.sleep(0.2)
                
                current_date += timedelta(days=1)
            
            # Calculate monthly statistics
            if month_gappers:
                # Get intraday averages
                time_aggregates = {}
                for gapper in month_gappers:
                    for point in gapper.get('intraday_data', []):
                        time_key = point['time']
                        if time_key not in time_aggregates:
                            time_aggregates[time_key] = []
                        time_aggregates[time_key].append(point['price_change_pct'])
                
                intraday_data = []
                for time_key in sorted(time_aggregates.keys()):
                    intraday_data.append({
                        'time': time_key,
                        'avgPriceChange': np.mean(time_aggregates[time_key]),
                        'volume': len(time_aggregates[time_key]) * 1000000
                    })
                
                monthly_data.append({
                    'month': month_start.strftime('%b'),
                    'year': month_start.year,
                    'intradayData': intraday_data,
                    'statistics': self.calculate_monthly_statistics(month_gappers)
                })
        
        # Get last 20 gaps
        last_gaps = []
        for gapper in all_gappers[-20:]:
            last_gaps.append({
                'ticker': gapper['ticker'],
                'gapPercentage': gapper['gap_percentage'],
                'volume': gapper['volume'],
                'date': gapper['date']
            })
        
        # Calculate overall statistics
        overall_stats = {
            'totalGappers': len(all_gappers),
            'avgOpenToClose': np.mean([m['statistics']['avgOpenToClose'] for m in monthly_data if m['statistics']]),
            'medianHighTime': self.get_most_common_time([m['statistics']['medianHighTime'] for m in monthly_data if m['statistics']]),
            'profitableShorts': np.mean([m['statistics']['profitableShorts'] for m in monthly_data if m['statistics']])
        }
        
        return {
            'last_updated': datetime.now().isoformat(),
            'monthlyData': monthly_data[::-1],  # Reverse to show oldest first
            'lastGaps': last_gaps,
            'overallStats': overall_stats
        }
    
    def get_previous_trading_day(self, date_str: str) -> str:
        """Get previous trading day"""
        date = datetime.strptime(date_str, '%Y-%m-%d')
        date -= timedelta(days=1)
        while date.weekday() >= 5:  # Skip weekends
            date -= timedelta(days=1)
        return date.strftime('%Y-%m-%d')
    
    def get_most_common_time(self, times: List[str]) -> str:
        """Get most common time from list"""
        if not times:
            return "10:00"
        from collections import Counter
        return Counter(times).most_common(1)[0][0]
    
    def save_data(self, data: Dict, output_path: str = "data/gapper_stats.json"):
        """Save data to JSON file"""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Data saved to {output_path}")

def main():
    # Your Polygon API key
    API_KEY = "YOUR_POLYGON_API_KEY_HERE"
    
    collector = GapperDataCollector(API_KEY)
    
    print("Starting gapper data collection...")
    data = collector.collect_data(lookback_months=12)
    
    collector.save_data(data)
    print("Data collection complete!")

if __name__ == "__main__":
    main()
