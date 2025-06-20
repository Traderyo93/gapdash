#!/usr/bin/env python3
"""
Gap Scanner Data Updater
Updates gap scanner data from Polygon.io API
"""

import os
import sys
import json
import requests
from datetime import datetime, timedelta
import pytz
from collections import defaultdict
import time

class GapDataUpdater:
    def __init__(self):
        self.api_key = os.getenv('POLYGON_API_KEY')
        if not self.api_key:
            raise ValueError("POLYGON_API_KEY environment variable not set")
        
        self.eastern = pytz.timezone('US/Eastern')
        self.data_dir = 'data'
        self.cache_file = os.path.join(self.data_dir, 'gap_data_cache.json')
        
        # Ensure data directory exists
        os.makedirs(self.data_dir, exist_ok=True)
        
    def is_market_day(self, date):
        """Check if date is a trading day"""
        # Skip weekends
        if date.weekday() >= 5:
            return False
        
        # Add holiday check here if needed
        holidays = [
            # Add US market holidays
        ]
        
        return date.date() not in holidays
    
    def get_trading_days(self, days=90):
        """Get list of trading days for the past N days"""
        trading_days = []
        current = datetime.now(self.eastern)
        
        while len(trading_days) < days:
            if self.is_market_day(current):
                trading_days.append(current.strftime('%Y-%m-%d'))
            current -= timedelta(days=1)
            
        return trading_days
    
    def fetch_previous_close(self, ticker, date):
        """Fetch previous close for a ticker"""
        # Get previous trading day
        current_date = datetime.strptime(date, '%Y-%m-%d')
        prev_date = current_date - timedelta(days=1)
        
        # Skip back to find previous trading day
        while not self.is_market_day(prev_date):
            prev_date -= timedelta(days=1)
        
        prev_date_str = prev_date.strftime('%Y-%m-%d')
        
        url = f"https://api.polygon.io/v1/open-close/{ticker}/{prev_date_str}"
        params = {'adjusted': 'false', 'apiKey': self.api_key}
        
        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                return data.get('close')
        except Exception as e:
            pass
            
        return None
    
    def fetch_daily_gappers(self, date):
        """Fetch gappers for a specific date"""
        url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}"
        params = {
            'adjusted': 'false',
            'apiKey': self.api_key
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            print(f"API response for {date}: {len(data.get('results', []))} stocks")
            
            if data.get('results'):
                # Process the grouped data
                quotes_data = {}
                for result in data['results']:
                    ticker = result.get('T', '')
                    if ticker and len(ticker) <= 5:  # Filter out weird tickers
                        quotes_data[ticker] = {
                            'o': result.get('o', 0),
                            'h': result.get('h', 0),
                            'l': result.get('l', 0),
                            'c': result.get('c', 0),
                            'v': result.get('v', 0),
                            'pc': result.get('pc', 0),  # Previous close
                            'date': date
                        }
                
                print(f"Processing {len(quotes_data)} tickers for {date}")
                return self.process_daily_gappers(quotes_data, date)
            
        except Exception as e:
            print(f"Error fetching data for {date}: {e}")
            
        return []
    
    def process_daily_gappers(self, quotes_data, date):
        """Process quotes data to find genuine gappers (excluding splits)"""
        gappers = []
        processed_count = 0
        gap_candidates = 0
        
        for ticker, data in quotes_data.items():
            processed_count += 1
            try:
                # Current day data
                open_price = data.get('o', 0)
                high = data.get('h', 0)
                low = data.get('l', 0)
                close = data.get('c', 0)
                volume = data.get('v', 0)
                prev_close = data.get('pc', 0)
                
                # Skip if no volume or price data
                if volume < 100000 or open_price <= 0:
                    continue
                
                # If no previous close in data, try to fetch it
                if not prev_close or prev_close <= 0:
                    prev_close = self.fetch_previous_close(ticker, date)
                    if processed_count <= 50:  # Only try for first 50 to avoid rate limits
                        time.sleep(0.1)  # Rate limit
                
                if prev_close and prev_close > 0 and open_price > 0:
                    gap_pct = ((open_price - prev_close) / prev_close) * 100
                    
                    # Only check gaps that meet minimum criteria first
                    if abs(gap_pct) >= 25:  # Lower threshold for debugging
                        gap_candidates += 1
                        
                        print(f"Gap candidate: {ticker} - {gap_pct:.1f}% (Vol: {volume:,.0f})")
                        
                        # CRITICAL: Filter out splits and corporate actions
                        if abs(gap_pct) > 300:
                            print(f"  -> Skipping {ticker}: {gap_pct:.1f}% gap likely a split/corporate action")
                            continue
                        
                        # Check if the ratio suggests a common split
                        ratio = open_price / prev_close
                        common_splits = [2.0, 3.0, 4.0, 5.0, 10.0, 0.5, 0.33, 0.25, 0.2, 0.1]
                        
                        is_split = False
                        for split_ratio in common_splits:
                            if abs(ratio - split_ratio) < 0.05:  # 5% tolerance
                                print(f"  -> Skipping {ticker}: Detected likely {split_ratio}:1 split")
                                is_split = True
                                break
                        
                        if is_split:
                            continue
                        
                        # Additional sanity check for large gaps
                        if gap_pct > 100 and volume < 500000:
                            print(f"  -> Skipping {ticker}: Large gap with low volume, likely not real")
                            continue
                        
                        # Filter for upward gaps meeting criteria
                        if gap_pct >= 50 and open_price >= 0.30 and volume >= 1000000:
                            print(f"  -> ✓ GAPPER FOUND: {ticker} - {gap_pct:.1f}%")
                            gappers.append({
                                'ticker': ticker,
                                'gap_percentage': round(gap_pct, 2),
                                'open': open_price,
                                'high': high,
                                'low': low,
                                'close': close,
                                'price': close,
                                'volume': volume,
                                'prev_close': prev_close,
                                'date': date
                            })
                        
            except Exception as e:
                print(f"Error processing {ticker}: {e}")
                continue
        
        print(f"Date {date}: Processed {processed_count} tickers, found {gap_candidates} gap candidates, {len(gappers)} final gappers")
        
        # Sort by gap percentage (highest first)
        gappers.sort(key=lambda x: x['gap_percentage'], reverse=True)
        
        return gappers
    
    def fetch_todays_gappers(self):
        """Get today's actual gappers for Last Gaps section"""
        today = datetime.now(self.eastern)
        
        # Skip weekends - go to most recent trading day
        while today.weekday() >= 5:
            today = today - timedelta(days=1)
        
        date_str = today.strftime('%Y-%m-%d')
        
        print(f"Fetching today's gappers for {date_str}")
        gappers = self.fetch_daily_gappers(date_str)
        
        # If no gappers today, try yesterday
        if not gappers:
            yesterday = today - timedelta(days=1)
            while yesterday.weekday() >= 5:
                yesterday = yesterday - timedelta(days=1)
            date_str = yesterday.strftime('%Y-%m-%d')
            print(f"No gappers today, trying {date_str}")
            gappers = self.fetch_daily_gappers(date_str)
        
        # Format for dashboard with dates
        return [{
            'ticker': g['ticker'],
            'gapPercentage': g['gap_percentage'],
            'volume': g['volume'],
            'date': g['date'],
            'open': g['open'],
            'high': g['high'],
            'low': g['low'],
            'close': g['close']
        } for g in gappers[:20]]  # Top 20 gappers
    
    def fetch_intraday_data(self, ticker, date):
        """Fetch intraday 15-minute data for candlestick charts"""
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/15/minute/{date}/{date}"
        params = {
            'adjusted': 'false',
            'sort': 'asc',
            'apiKey': self.api_key
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if data.get('results'):
                # Filter for market hours (4 AM to 10 PM ET)
                candles = []
                for candle in data['results']:
                    timestamp = candle['t']
                    dt = datetime.fromtimestamp(timestamp/1000, tz=self.eastern)
                    hour = dt.hour
                    
                    # Include data from 4 AM to 10 PM
                    if 4 <= hour < 22:
                        candles.append({
                            'time': dt.strftime('%H:%M'),
                            'timestamp': timestamp,
                            'open': candle['o'],
                            'high': candle['h'],
                            'low': candle['l'],
                            'close': candle['c'],
                            'volume': candle['v']
                        })
                
                return candles
                
        except Exception as e:
            print(f"Error fetching intraday data for {ticker} on {date}: {e}")
            
        return []
    
    def daily_update(self):
        """Main update function - runs daily"""
        print(f"Starting daily update at {datetime.now()}")
        
        # Initialize cache structure
        cache_data = {
            'lastUpdated': datetime.now().isoformat(),
            'gappers': {},
            'stats': {},
            'lastGaps': [],
            'intradayData': {}
        }
        
        # Get today's real gappers for Last Gaps section
        print("Fetching today's gappers for Last Gaps...")
        todays_gappers = self.fetch_todays_gappers()
        cache_data['lastGaps'] = todays_gappers
        print(f"Found {len(todays_gappers)} gappers for today")
        
        # Get historical data for past 30 days (reduced for faster testing)
        trading_days = self.get_trading_days(30)
        all_gappers = []
        daily_counts = []
        
        print(f"Fetching data for {len(trading_days)} trading days...")
        
        for i, date in enumerate(trading_days):
            if i % 5 == 0:
                print(f"Processing day {i+1}/{len(trading_days)}...")
                
            gappers = self.fetch_daily_gappers(date)
            
            if gappers:
                all_gappers.extend(gappers)
                daily_counts.append({
                    'date': date,
                    'count': len(gappers)
                })
                
                # Store gappers by date
                cache_data['gappers'][date] = gappers[:50]  # Top 50 per day
            
            # Rate limit: 5 requests per second on free tier
            time.sleep(0.3)
        
        # Calculate statistics
        total_gaps = len(all_gappers)
        print(f"Total gappers found across all days: {total_gaps}")
        
        if all_gappers:
            # Average gaps per day
            avg_gaps_per_day = len(all_gappers) / len(trading_days)
            
            # Find biggest gappers
            all_gappers.sort(key=lambda x: x['gap_percentage'], reverse=True)
            top_gappers = all_gappers[:100]
            
            # Ticker frequency
            ticker_counts = defaultdict(int)
            for gapper in all_gappers:
                ticker_counts[gapper['ticker']] += 1
            
            most_frequent = sorted(ticker_counts.items(), key=lambda x: x[1], reverse=True)[:20]
            
            cache_data['stats'] = {
                'averageGapsPerDay': round(avg_gaps_per_day, 2),
                'totalGappers': len(all_gappers),
                'biggestGappers': top_gappers,
                'mostFrequent': [{'ticker': t, 'count': c} for t, c in most_frequent],
                'dailyCounts': daily_counts
            }
        else:
            # Provide empty stats if no gappers found
            cache_data['stats'] = {
                'averageGapsPerDay': 0,
                'totalGappers': 0,
                'biggestGappers': [],
                'mostFrequent': [],
                'dailyCounts': []
            }
        
        # Fetch intraday data for today's top gappers
        if todays_gappers:
            print("Fetching intraday data for top gappers...")
            today_str = datetime.now(self.eastern).strftime('%Y-%m-%d')
            
            for i, gapper in enumerate(todays_gappers[:3]):  # Top 3 for intraday
                ticker = gapper['ticker']
                print(f"Fetching intraday data for {ticker}...")
                
                intraday = self.fetch_intraday_data(ticker, today_str)
                if intraday:
                    cache_data['intradayData'][ticker] = {
                        'data': intraday,
                        'date': today_str,
                        'gapPercentage': gapper['gapPercentage']
                    }
                
                time.sleep(0.3)  # Rate limit
        
        # Save to cache file
        with open(self.cache_file, 'w') as f:
            json.dump(cache_data, f, indent=2)
            
        print(f"Update complete! Data saved to {self.cache_file}")
        print(f"Found {len(all_gappers)} total gappers over {len(trading_days)} days")
        print(f"Today's gappers: {len(todays_gappers)}")
        
        # Verify file was created
        if os.path.exists(self.cache_file):
            file_size = os.path.getsize(self.cache_file)
            print(f"Cache file created successfully: {file_size} bytes")
        else:
            print("ERROR: Cache file was not created!")
        
    def test_connection(self):
        """Test API connection"""
        url = "https://api.polygon.io/v1/marketstatus/now"
        params = {'apiKey': self.api_key}
        
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            print("API connection successful!")
            print(f"Market status: {json.dumps(data, indent=2)}")
            return True
        except Exception as e:
            print(f"API connection failed: {e}")
            return False

def main():
    """Main entry point"""
    updater = GapDataUpdater()
    
    # Test connection first
    if not updater.test_connection():
        sys.exit(1)
    
    # Run daily update
    updater.daily_update()

if __name__ == "__main__":
    main()
