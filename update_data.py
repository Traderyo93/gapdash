#!/usr/bin/env python3
"""
Gapper Data Manager - Handles initial historical load and daily updates
"""

import pandas as pd
import numpy as np
import requests
import json
import os
from datetime import datetime, timedelta, time
import pytz
import time as time_module
import logging

class GapperDataManager:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.polygon.io/v2"
        self.eastern = pytz.timezone('US/Eastern')
        self.cache_file = "data/gapper_stats.json"
        
        # Set up logging
        logging.basicConfig(level=logging.INFO, 
                          format='%(asctime)s - %(levelname)s - %(message)s')
    
    def load_cache(self):
        """Load existing cache data"""
        if os.path.exists(self.cache_file):
            with open(self.cache_file, 'r') as f:
                return json.load(f)
        return None
    
    def save_cache(self, data):
        """Save data to cache file"""
        os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
        with open(self.cache_file, 'w') as f:
            json.dump(data, f, indent=2)
        logging.info(f"Cache saved to {self.cache_file}")
    
    def get_previous_trading_day(self, date_str):
        """Get previous trading day"""
        date = datetime.strptime(date_str, '%Y-%m-%d')
        previous_day = date - timedelta(days=1)
        while previous_day.weekday() >= 5:
            previous_day -= timedelta(days=1)
        return previous_day.strftime('%Y-%m-%d')
    
    def fetch_daily_gappers(self, date_str):
        """Fetch gappers for a specific date using same logic as backtest"""
        try:
            prev_date_str = self.get_previous_trading_day(date_str)
            
            # Get previous closes
            prev_url = f"{self.base_url}/aggs/grouped/locale/us/market/stocks/{prev_date_str}?adjusted=false&apiKey={self.api_key}"
            prev_response = requests.get(prev_url)
            prev_data = prev_response.json()
            
            prev_closes = {}
            if 'results' in prev_data:
                for stock in prev_data['results']:
                    prev_closes[stock['T']] = stock['c']
            
            # Get current day data
            curr_url = f"{self.base_url}/aggs/grouped/locale/us/market/stocks/{date_str}?adjusted=false&apiKey={self.api_key}"
            curr_response = requests.get(curr_url)
            curr_data = curr_response.json()
            
            gappers = []
            if 'results' in curr_data:
                for stock in curr_data['results']:
                    ticker = stock['T']
                    
                    # Apply filters
                    if len(ticker) > 4 or ticker.endswith(('WS', 'RT', 'WSA')):
                        continue
                    
                    if ticker in prev_closes:
                        prev_close = prev_closes[ticker]
                        open_price = stock['o']
                        volume = stock['v']
                        
                        # Calculate gap
                        gap_pct = ((open_price - prev_close) / prev_close) * 100
                        
                        # Check criteria: 50%+ gap, $0.30+ price, 1M+ volume
                        if abs(gap_pct) >= 50 and open_price >= 0.30 and volume >= 1000000:
                            gappers.append({
                                'ticker': ticker,
                                'gap_percentage': gap_pct,
                                'volume': volume,
                                'price': stock['c'],
                                'open': open_price,
                                'high': stock['h'],
                                'low': stock['l'],
                                'previous_close': prev_close,
                                'date': date_str
                            })
            
            return sorted(gappers, key=lambda x: abs(x['gap_percentage']), reverse=True)
            
        except Exception as e:
            logging.error(f"Error fetching gappers for {date_str}: {e}")
            return []
    
    def fetch_intraday_patterns(self, ticker, date_str):
        """Fetch 5-minute intraday data and calculate patterns"""
        url = f"{self.base_url}/aggs/ticker/{ticker}/range/5/minute/{date_str}/{date_str}?adjusted=false&sort=asc&apiKey={self.api_key}"
        
        try:
            response = requests.get(url)
            data = response.json()
            
            if data.get('status') == 'OK' and data.get('results'):
                df = pd.DataFrame(data['results'])
                df['t'] = pd.to_datetime(df['t'], unit='ms', utc=True).dt.tz_convert(self.eastern)
                df['time'] = df['t'].dt.strftime('%H:%M')
                
                # Get market open price
                market_data = df[df['t'].dt.time >= time(9, 30)]
                if not market_data.empty:
                    open_price = market_data.iloc[0]['o']
                    
                    # Calculate price change from open
                    price_changes = []
                    for _, row in df.iterrows():
                        if row['t'].time() >= time(9, 30) and row['t'].time() <= time(16, 0):
                            pct_change = ((row['c'] - open_price) / open_price) * 100
                            price_changes.append({
                                'time': row['time'],
                                'price_change_pct': pct_change
                            })
                    
                    return price_changes
                    
        except Exception as e:
            logging.error(f"Error fetching intraday data for {ticker}: {e}")
            
        return []
    
    def initial_historical_load(self, months=12):
        """Load historical data for the specified number of months"""
        logging.info(f"Starting initial historical load for {months} months...")
        
        end_date = datetime.now(self.eastern)
        start_date = end_date - timedelta(days=30 * months)
        
        all_data = {
            'monthlyData': [],
            'lastGaps': [],
            'overallStats': {},
            'last_updated': datetime.now().isoformat()
        }
        
        # Process each month
        current_date = start_date
        monthly_aggregates = {}
        
        while current_date <= end_date:
            if current_date.weekday() < 5:  # Weekday
                date_str = current_date.strftime('%Y-%m-%d')
                month_key = current_date.strftime('%Y-%m')
                
                logging.info(f"Fetching data for {date_str}...")
                
                # Get gappers for this day
                daily_gappers = self.fetch_daily_gappers(date_str)
                
                if daily_gappers:
                    if month_key not in monthly_aggregates:
                        monthly_aggregates[month_key] = {
                            'gappers': [],
                            'intraday_patterns': {}
                        }
                    
                    monthly_aggregates[month_key]['gappers'].extend(daily_gappers)
                    
                    # Get intraday patterns for top gappers (limit API calls)
                    for gapper in daily_gappers[:3]:  # Top 3 gappers per day
                        patterns = self.fetch_intraday_patterns(gapper['ticker'], date_str)
                        
                        for pattern in patterns:
                            time_key = pattern['time']
                            if time_key not in monthly_aggregates[month_key]['intraday_patterns']:
                                monthly_aggregates[month_key]['intraday_patterns'][time_key] = []
                            monthly_aggregates[month_key]['intraday_patterns'][time_key].append(pattern['price_change_pct'])
                        
                        time_module.sleep(0.12)  # Rate limiting
                
                time_module.sleep(0.12)  # Rate limiting between days
            
            current_date += timedelta(days=1)
        
        # Process monthly aggregates
        for month_key, month_data in monthly_aggregates.items():
            if month_data['gappers']:
                # Calculate average intraday patterns
                intraday_avg = []
                for time_key in sorted(month_data['intraday_patterns'].keys()):
                    values = month_data['intraday_patterns'][time_key]
                    intraday_avg.append({
                        'time': time_key,
                        'avgPriceChange': np.mean(values),
                        'volume': len(values) * 1000000
                    })
                
                # Find high/low points
                if intraday_avg:
                    high_point = max(intraday_avg, key=lambda x: x['avgPriceChange'])
                    low_point = min(intraday_avg, key=lambda x: x['avgPriceChange'])
                    
                    month_date = datetime.strptime(month_key + '-01', '%Y-%m-%d')
                    
                    all_data['monthlyData'].append({
                        'month': month_date.strftime('%b'),
                        'year': month_date.year,
                        'intradayData': intraday_avg,
                        'statistics': {
                            'totalGappers': len(month_data['gappers']),
                            'avgOpenToClose': intraday_avg[-1]['avgPriceChange'] if intraday_avg else 0,
                            'medianHighTime': high_point['time'],
                            'medianHighValue': high_point['avgPriceChange'],
                            'medianLowValue': low_point['avgPriceChange'],
                            'profitableShorts': sum(1 for g in month_data['gappers'] if g['gap_percentage'] < 0) / len(month_data['gappers']) * 100
                        }
                    })
        
        # Get last 20 gaps from most recent data
        all_gappers = []
        for month_data in monthly_aggregates.values():
            all_gappers.extend(month_data['gappers'])
        
        all_gappers.sort(key=lambda x: x['date'], reverse=True)
        all_data['lastGaps'] = [{
            'ticker': g['ticker'],
            'gapPercentage': g['gap_percentage'],
            'volume': g['volume'],
            'date': g['date']
        } for g in all_gappers[:20]]
        
        # Calculate overall stats
        if all_data['monthlyData']:
            all_data['overallStats'] = {
                'totalGappers': sum(m['statistics']['totalGappers'] for m in all_data['monthlyData']),
                'avgOpenToClose': np.mean([m['statistics']['avgOpenToClose'] for m in all_data['monthlyData']]),
                'medianHighTime': "10:45",
                'profitableShorts': np.mean([m['statistics']['profitableShorts'] for m in all_data['monthlyData']])
            }
        
        self.save_cache(all_data)
        logging.info("Initial historical load complete!")
        return all_data
    
    def daily_update(self):
        """Update cache with today's data"""
        logging.info("Running daily update...")
        
        # Load existing cache
        cache_data = self.load_cache()
        if not cache_data:
            logging.info("No cache found, running initial load...")
            return self.initial_historical_load()
        
        # Get today's data
        today = datetime.now(self.eastern)
        date_str = today.strftime('%Y-%m-%d')
        
        # Skip weekends
        if today.weekday() >= 5:
            logging.info("Weekend - skipping update")
            return cache_data
        
        # Fetch today's gappers
        today_gappers = self.fetch_daily_gappers(date_str)
        
        if today_gappers:
            logging.info(f"Found {len(today_gappers)} gappers today")
            
            # Update last gaps
            cache_data['lastGaps'] = [{
                'ticker': g['ticker'],
                'gapPercentage': g['gap_percentage'],
                'volume': g['volume'],
                'date': g['date']
            } for g in today_gappers[:20]]
            
            # Update current month's data
            month_key = today.strftime('%b')
            year = today.year
            
            # Find current month in cache
            current_month_index = None
            for i, month_data in enumerate(cache_data['monthlyData']):
                if month_data['month'] == month_key and month_data['year'] == year:
                    current_month_index = i
                    break
            
            # Collect intraday patterns for today
            intraday_patterns = {}
            for gapper in today_gappers[:5]:  # Top 5 gappers
                patterns = self.fetch_intraday_patterns(gapper['ticker'], date_str)
                
                for pattern in patterns:
                    time_key = pattern['time']
                    if time_key not in intraday_patterns:
                        intraday_patterns[time_key] = []
                    intraday_patterns[time_key].append(pattern['price_change_pct'])
                
                time_module.sleep(0.12)
            
            # Calculate today's averages
            if intraday_patterns:
                intraday_avg = []
                for time_key in sorted(intraday_patterns.keys()):
                    values = intraday_patterns[time_key]
                    intraday_avg.append({
                        'time': time_key,
                        'avgPriceChange': np.mean(values),
                        'volume': len(values) * 1000000
                    })
                
                # Update or create month data
                if current_month_index is not None:
                    # Merge with existing data
                    existing_data = cache_data['monthlyData'][current_month_index]['intradayData']
                    
                    # Weighted average of existing and new data
                    for new_point in intraday_avg:
                        found = False
                        for existing_point in existing_data:
                            if existing_point['time'] == new_point['time']:
                                # Average the values
                                existing_point['avgPriceChange'] = (existing_point['avgPriceChange'] + new_point['avgPriceChange']) / 2
                                found = True
                                break
                        
                        if not found:
                            existing_data.append(new_point)
                    
                    # Re-sort by time
                    existing_data.sort(key=lambda x: x['time'])
                    
                    # Update statistics
                    cache_data['monthlyData'][current_month_index]['statistics']['totalGappers'] += len(today_gappers)
                    
                else:
                    # Create new month entry
                    high_point = max(intraday_avg, key=lambda x: x['avgPriceChange'])
                    low_point = min(intraday_avg, key=lambda x: x['avgPriceChange'])
                    
                    cache_data['monthlyData'].append({
                        'month': month_key,
                        'year': year,
                        'intradayData': intraday_avg,
                        'statistics': {
                            'totalGappers': len(today_gappers),
                            'avgOpenToClose': intraday_avg[-1]['avgPriceChange'] if intraday_avg else 0,
                            'medianHighTime': high_point['time'],
                            'medianHighValue': high_point['avgPriceChange'],
                            'medianLowValue': low_point['avgPriceChange'],
                            'profitableShorts': sum(1 for g in today_gappers if g['gap_percentage'] < 0) / len(today_gappers) * 100
                        }
                    })
            
            # Update overall stats
            cache_data['overallStats'] = {
                'totalGappers': sum(m['statistics']['totalGappers'] for m in cache_data['monthlyData']),
                'avgOpenToClose': np.mean([m['statistics']['avgOpenToClose'] for m in cache_data['monthlyData']]),
                'medianHighTime': "10:45",
                'profitableShorts': np.mean([m['statistics']['profitableShorts'] for m in cache_data['monthlyData']])
            }
            
            cache_data['last_updated'] = datetime.now().isoformat()
            
            self.save_cache(cache_data)
            logging.info("Daily update complete!")
        
        return cache_data

def main():
    # Get API key from environment variable
    API_KEY = os.environ.get('POLYGON_API_KEY')
    
    if not API_KEY:
        print("Error: POLYGON_API_KEY not set")
        return
    
    manager = GapperDataManager(API_KEY)
    
    # Check if we need initial load or just daily update
    cache_data = manager.load_cache()
    
    if not cache_data:
        print("No cache found - running initial historical load...")
        manager.initial_historical_load(months=12)
    else:
        print("Cache found - running daily update...")
        manager.daily_update()

if __name__ == "__main__":
    main()
