#!/usr/bin/env python3
"""
Gap Scanner Data Updater - Intraday Charts Format
Collects detailed 15-minute intraday data for dashboard charts
"""

import os
import sys
import json
import requests
from datetime import datetime, timedelta, time
import pytz
import pandas as pd
import time as time_module
from collections import defaultdict

class GapDataUpdater:
    def __init__(self):
        self.api_key = os.getenv('POLYGON_API_KEY')
        if not self.api_key:
            raise ValueError("POLYGON_API_KEY environment variable not set")
        
        self.eastern = pytz.timezone('US/Eastern')
        self.data_dir = 'data'
        self.cache_file = os.path.join(self.data_dir, 'gap_data_cache.json')
        
        os.makedirs(self.data_dir, exist_ok=True)
        self.polygon_base_url = "https://api.polygon.io/v2"
        
    def filter_ticker_symbols(self, ticker):
        """Exact same filter as your backtest"""
        invalid_suffixes = ('WS', 'RT', 'WSA')
        
        if len(ticker) >= 5:
            return False
        
        if any(ticker.endswith(suffix) for suffix in invalid_suffixes):
            return False
        
        if ticker in ['ZVZZT', 'ZWZZT', 'ZBZZT']:
            return False
        
        return True
    
    def get_previous_trading_day(self, date):
        """Get previous trading day"""
        eastern = pytz.timezone('US/Eastern')
        if date.tzinfo is None:
            date = eastern.localize(date)
        
        previous_day = date - timedelta(days=1)
        max_attempts = 10
        
        for _ in range(max_attempts):
            prev_date_str = previous_day.strftime('%Y-%m-%d')
            url = f"{self.polygon_base_url}/aggs/grouped/locale/us/market/stocks/{prev_date_str}?adjusted=false&apiKey={self.api_key}"
            
            try:
                response = requests.get(url)
                data = response.json()
                
                if 'results' in data and data['results']:
                    return previous_day
                
                previous_day -= timedelta(days=1)
                
            except Exception:
                previous_day -= timedelta(days=1)
        
        return None
    
    def fetch_detailed_intraday_data(self, ticker, date_str):
        """Fetch detailed minute-by-minute data for full day chart"""
        try:
            url = f"{self.polygon_base_url}/aggs/ticker/{ticker}/range/1/minute/{date_str}/{date_str}?adjusted=false&sort=asc&limit=50000&apiKey={self.api_key}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            if 'results' in data and data['results']:
                return data['results']
            
            return None
                
        except Exception as e:
            print(f"Error fetching detailed intraday data for {ticker}: {e}")
            return None

    def process_intraday_for_chart(self, intraday_data, ticker, date_str, open_price, gap_percentage):
        """Process minute data into 15-minute chart format"""
        try:
            df = pd.DataFrame(intraday_data)
            if df.empty:
                return None
                
            eastern = pytz.timezone('US/Eastern')
            df['t'] = pd.to_datetime(df['t'], unit='ms')
            if df['t'].dt.tz is None:
                df['t'] = df['t'].dt.tz_localize('UTC').dt.tz_convert(eastern)
            elif df['t'].dt.tz != eastern:
                df['t'] = df['t'].dt.tz_convert(eastern)
            
            # Filter to market hours (9:30 AM - 4:00 PM)
            market_hours = df[
                (df['t'].dt.time >= time(9, 30)) & 
                (df['t'].dt.time <= time(16, 0))
            ].copy()
            
            if market_hours.empty:
                return None
            
            # Resample to 15-minute intervals
            market_hours.set_index('t', inplace=True)
            resampled = market_hours.resample('15T').agg({
                'o': 'first',
                'h': 'max', 
                'l': 'min',
                'c': 'last',
                'v': 'sum'
            }).dropna()
            
            if resampled.empty:
                return None
            
            # Get day's high and low
            day_high = market_hours['h'].max()
            day_low = market_hours['l'].min()
            day_open = market_hours['o'].iloc[0]
            day_close = market_hours['c'].iloc[-1]
            
            # Calculate open to close change
            open_to_close_change = ((day_close - day_open) / day_open) * 100
            
            # Create time series data
            times = []
            prices = []
            volumes = []
            
            for timestamp, row in resampled.iterrows():
                times.append(timestamp.strftime('%H:%M'))
                prices.append(float(row['c']))
                volumes.append(int(row['v']))
            
            return {
                'ticker': ticker,
                'date': date_str,
                'gap_percentage': float(gap_percentage),
                'times': times,
                'prices': prices,
                'volumes': volumes,
                'open': float(day_open),
                'high': float(day_high), 
                'low': float(day_low),
                'close': float(day_close),
                'open_to_close_change': float(open_to_close_change),
                'total_volume': int(market_hours['v'].sum())
            }
            
        except Exception as e:
            print(f"Error processing intraday data for {ticker}: {e}")
            return None

    def fetch_candidates_for_date(self, date):
        """Find gappers and get their detailed intraday data"""
        try:
            date_str = date.strftime('%Y-%m-%d')
            print(f"\n=== Processing {date_str} ===")
            
            eastern = pytz.timezone('US/Eastern')
            if date.tzinfo is None:
                date = eastern.localize(date)
                
            previous_day = self.get_previous_trading_day(date)
            if previous_day is None:
                print(f"Could not find previous trading day for {date_str}")
                return []
                
            prev_date_str = previous_day.strftime('%Y-%m-%d')
            
            # Get previous day's closing prices
            prev_close_url = f"{self.polygon_base_url}/aggs/grouped/locale/us/market/stocks/{prev_date_str}?adjusted=false&type=CS,PS,ADR&apiKey={self.api_key}"
            prev_close_response = requests.get(prev_close_url)
            prev_close_response.raise_for_status()
            prev_close_data = prev_close_response.json()
            
            prev_closes = {stock['T']: stock['c'] for stock in prev_close_data.get('results', [])}
            
            # Get current date data
            current_url = f"{self.polygon_base_url}/aggs/grouped/locale/us/market/stocks/{date_str}?adjusted=false&type=CS,PS,ADR&apiKey={self.api_key}"
            current_response = requests.get(current_url)
            current_response.raise_for_status()
            current_data = current_response.json()
            
            initial_candidates = []
            
            # Find initial gap candidates
            if 'results' in current_data:
                for stock in current_data['results']:
                    ticker = stock['T']
                    opening = stock['o']
                    
                    if not self.filter_ticker_symbols(ticker):
                        continue
                        
                    if ticker in prev_closes:
                        prev_close = prev_closes[ticker]
                        initial_gap = ((opening - prev_close) / prev_close) * 100
                        
                        if initial_gap >= 45 and opening >= 0.30:
                            initial_candidates.append({
                                'ticker': ticker,
                                'previous_close': prev_close,
                                'initial_gap': initial_gap,
                                'opening': opening
                            })
            
            print(f"Found {len(initial_candidates)} potential gappers")
            
            # Process each candidate for detailed charts
            detailed_gappers = []
            for i, candidate in enumerate(initial_candidates):
                ticker = candidate['ticker']
                print(f"Processing {i+1}/{len(initial_candidates)}: {ticker}")
                
                # Get detailed intraday data
                intraday_data = self.fetch_detailed_intraday_data(ticker, date_str)
                if intraday_data:
                    # Check volume and final criteria
                    df = pd.DataFrame(intraday_data)
                    if not df.empty:
                        df['t'] = pd.to_datetime(df['t'], unit='ms')
                        df['t'] = df['t'].dt.tz_localize('UTC').dt.tz_convert(eastern)
                        
                        # Check pre-market volume
                        pre_market = df[df['t'].dt.time < time(9, 30)]
                        pre_market_volume = pre_market['v'].sum() if not pre_market.empty else 0
                        
                        if pre_market_volume >= 1000000:
                            # Process for chart
                            chart_data = self.process_intraday_for_chart(
                                intraday_data, 
                                ticker, 
                                date_str, 
                                candidate['opening'],
                                candidate['initial_gap']
                            )
                            
                            if chart_data:
                                print(f"  ✓ Added {ticker} with {len(chart_data['times'])} data points")
                                detailed_gappers.append(chart_data)
                            else:
                                print(f"  ✗ Failed to process chart data for {ticker}")
                        else:
                            print(f"  ✗ {ticker} insufficient volume: {pre_market_volume:,}")
                
                # Rate limiting
                time_module.sleep(0.12)
            
            print(f"Final result: {len(detailed_gappers)} detailed gappers")
            return detailed_gappers
            
        except Exception as e:
            print(f"Error processing date {date_str}: {str(e)}")
            import traceback
            traceback.print_exc()
            return []
    
    def get_trading_days(self, days=90):
        """Get recent trading days (3 months for detailed charts)"""
        trading_days = []
        current_date = datetime.now(self.eastern)
        
        while len(trading_days) < days:
            if current_date.weekday() < 5:  # Monday = 0, Friday = 4
                trading_days.append(current_date)
            current_date -= timedelta(days=1)
        
        return list(reversed(trading_days))
    
    def organize_by_time_periods(self, all_gappers):
        """Organize gappers by different time periods for D/W/M toggles"""
        daily_data = defaultdict(list)
        weekly_data = defaultdict(list) 
        monthly_data = defaultdict(list)
        
        for gapper in all_gappers:
            date = datetime.strptime(gapper['date'], '%Y-%m-%d')
            
            # Daily organization (by date)
            daily_key = gapper['date']
            daily_data[daily_key].append(gapper)
            
            # Weekly organization
            year, week, _ = date.isocalendar()
            weekly_key = f"{year}-W{week:02d}"
            weekly_data[weekly_key].append(gapper)
            
            # Monthly organization
            monthly_key = f"{date.year}-{date.month:02d}"
            monthly_data[monthly_key].append(gapper)
        
        return {
            'daily': dict(daily_data),
            'weekly': dict(weekly_data), 
            'monthly': dict(monthly_data)
        }
    
    def calculate_summary_stats(self, all_gappers):
        """Calculate summary statistics for dashboard"""
        if not all_gappers:
            return {
                'total_gappers': 0,
                'avg_gap_percentage': 0,
                'avg_open_to_close': 0,
                'total_volume': 0,
                'days_since_last_gap': 0
            }
        
        # Calculate averages
        total_gappers = len(all_gappers)
        avg_gap = sum(g['gap_percentage'] for g in all_gappers) / total_gappers
        avg_otc = sum(g['open_to_close_change'] for g in all_gappers) / total_gappers
        total_vol = sum(g['total_volume'] for g in all_gappers)
        
        # Days since last gap
        latest_date = max(g['date'] for g in all_gappers)
        latest = datetime.strptime(latest_date, '%Y-%m-%d').date()
        today = datetime.now().date()
        days_since = (today - latest).days
        
        return {
            'total_gappers': total_gappers,
            'avg_gap_percentage': round(avg_gap, 2),
            'avg_open_to_close': round(avg_otc, 2),
            'total_volume': total_vol,
            'days_since_last_gap': max(0, days_since)
        }
    
    def daily_update(self):
        """Main update function"""
        print(f"🚀 Starting Detailed Gap Scanner Update at {datetime.now()}")
        
        # Test API connection
        try:
            test_url = "https://api.polygon.io/v1/marketstatus/now"
            test_response = requests.get(test_url, params={'apiKey': self.api_key}, timeout=10)
            test_response.raise_for_status()
            print("✓ API connection successful!")
        except Exception as e:
            print(f"❌ API connection failed: {e}")
            return
        
        # Get recent trading days
        trading_days = self.get_trading_days(90)  # 3 months of data
        print(f"Processing {len(trading_days)} trading days...")
        
        all_gappers = []
        
        # Process each trading day
        for i, date in enumerate(trading_days):
            print(f"\nDay {i+1}/{len(trading_days)}: {date.strftime('%Y-%m-%d')}")
            
            daily_gappers = self.fetch_candidates_for_date(date)
            if daily_gappers:
                all_gappers.extend(daily_gappers)
        
        print(f"\n📊 Processing {len(all_gappers)} total gappers...")
        
        # Organize data by time periods
        time_periods = self.organize_by_time_periods(all_gappers)
        summary_stats = self.calculate_summary_stats(all_gappers)
        
        # Get last gaps for sidebar
        recent_gappers = sorted(all_gappers, key=lambda x: x['date'], reverse=True)[:30]
        
        # Prepare cache data for new dashboard format
        cache_data = {
            'lastUpdated': datetime.now().isoformat(),
            'timePeriodsData': time_periods,
            'summaryStats': summary_stats,
            'lastGaps': [{
                'ticker': g['ticker'],
                'gapPercentage': g['gap_percentage'],
                'volume': g['total_volume'],
                'date': g['date'],
                'openToCloseChange': g['open_to_close_change']
            } for g in recent_gappers],
            'totalGappers': len(all_gappers)
        }
        
        # Save to cache file
        with open(self.cache_file, 'w') as f:
            json.dump(cache_data, f, indent=2)
            
        print(f"\n✅ UPDATE COMPLETE!")
        print(f"📁 Results saved to: {self.cache_file}")
        print(f"📊 Total gappers processed: {len(all_gappers)}")
        print(f"📅 Daily periods: {len(time_periods['daily'])}")
        print(f"📅 Weekly periods: {len(time_periods['weekly'])}")
        print(f"📅 Monthly periods: {len(time_periods['monthly'])}")
        
        # Verify file was created
        if os.path.exists(self.cache_file):
            file_size = os.path.getsize(self.cache_file)
            print(f"✓ Cache file created successfully: {file_size:,} bytes")
        else:
            print("❌ ERROR: Cache file was not created!")

def main():
    """Main entry point"""
    updater = GapDataUpdater()
    updater.daily_update()

if __name__ == "__main__":
    main()
