#!/usr/bin/env python3
"""
Gap Scanner Data Updater - Uses exact same logic as consolidated backtest
Updates gap scanner data from Polygon.io API
"""

import os
import sys
import json
import requests
from datetime import datetime, timedelta, time
import pytz
import pandas as pd
import time as time_module

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
        
        # API configuration
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
        """Exact same logic as your backtest"""
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
    
    def fetch_intraday_data(self, ticker, date_str):
        """Fetch minute-by-minute data for 9:28 candle logic"""
        try:
            url = f"{self.polygon_base_url}/aggs/ticker/{ticker}/range/1/minute/{date_str}/{date_str}?adjusted=false&sort=asc&limit=50000&apiKey={self.api_key}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            if 'results' in data and data['results']:
                return data['results']
            
            return None
                
        except Exception as e:
            print(f"Error fetching intraday data for {ticker}: {e}")
            return None
    
    def fetch_candidates_for_date(self, date):
        """EXACT same logic as your consolidated backtest"""
        try:
            date_str = date.strftime('%Y-%m-%d')
            print(f"\n=== Fetching candidates for {date_str} ===")
            
            eastern = pytz.timezone('US/Eastern')
            if date.tzinfo is None:
                date = eastern.localize(date)
                
            previous_day = self.get_previous_trading_day(date)
            if previous_day is None:
                print(f"Could not find previous trading day for {date_str}")
                return []
                
            prev_date_str = previous_day.strftime('%Y-%m-%d')
            print(f"Previous trading day: {prev_date_str}")
            
            # Get previous day's closing prices
            print(f"Fetching previous day closes from {prev_date_str}...")
            prev_close_url = f"{self.polygon_base_url}/aggs/grouped/locale/us/market/stocks/{prev_date_str}?adjusted=false&type=CS,PS,ADR&apiKey={self.api_key}"
            prev_close_response = requests.get(prev_close_url)
            prev_close_response.raise_for_status()
            prev_close_data = prev_close_response.json()
            
            prev_closes = {stock['T']: stock['c'] for stock in prev_close_data.get('results', [])}
            print(f"Got {len(prev_closes)} previous close prices")
            initial_candidates = []
            
            # Get current date data
            print(f"Fetching current day data from {date_str}...")
            current_url = f"{self.polygon_base_url}/aggs/grouped/locale/us/market/stocks/{date_str}?adjusted=false&type=CS,PS,ADR&apiKey={self.api_key}"
            current_response = requests.get(current_url)
            current_response.raise_for_status()
            current_data = current_response.json()
            
            print(f"Got {len(current_data.get('results', []))} stocks for current day")
            
            # First screening using daily data
            if 'results' in current_data:
                for stock in current_data['results']:
                    ticker = stock['T']
                    opening = stock['o']
                    
                    if not self.filter_ticker_symbols(ticker):
                        continue
                        
                    if ticker in prev_closes:
                        prev_close = prev_closes[ticker]
                        initial_gap = ((opening - prev_close) / prev_close) * 100
                        
                        needs_split_check = initial_gap >= 500
                        
                        if initial_gap >= 45 and opening >= 0.30:
                            initial_candidates.append({
                                'ticker': ticker,
                                'previous_close': prev_close,
                                'initial_gap': initial_gap,
                                'opening': opening,
                                'needs_split_check': needs_split_check
                            })
            
            print(f"Found {len(initial_candidates)} initial candidates")
            
            # Process intraday data for all initial candidates
            final_candidates = []
            for i, candidate in enumerate(initial_candidates):
                ticker = candidate['ticker']
                print(f"Processing {i+1}/{len(initial_candidates)}: {ticker} (initial gap: {candidate['initial_gap']:.1f}%)")
                
                intraday_data = self.fetch_intraday_data(ticker, date_str)
                if intraday_data is not None:
                    df = pd.DataFrame(intraday_data)
                    if not df.empty:
                        df['t'] = pd.to_datetime(df['t'], unit='ms')
                        if df['t'].dt.tz is None:
                            df['t'] = df['t'].dt.tz_localize('UTC').dt.tz_convert(eastern)
                        elif df['t'].dt.tz != eastern:
                            df['t'] = df['t'].dt.tz_convert(eastern)
                        
                        # Get 9:28 candle with fallback (EXACT same logic)
                        candle_928 = df[df['t'].dt.time == time(9, 28)]
                        
                        if candle_928.empty:
                            candle_929 = df[df['t'].dt.time == time(9, 29)]
                            if not candle_929.empty:
                                candle_928 = candle_929
                            else:
                                candle_927 = df[df['t'].dt.time == time(9, 27)]
                                if not candle_927.empty:
                                    candle_928 = candle_927
                        
                        if not candle_928.empty:
                            price_928 = candle_928.iloc[0]['c']
                            gap_928 = ((price_928 - candidate['previous_close']) / candidate['previous_close']) * 100
                            
                            print(f"  9:28 price: ${price_928:.4f}, Gap: {gap_928:.1f}%")
                            
                            # Check for split if suspicious gap (EXACT same logic)
                            if candidate['needs_split_check'] or gap_928 > 500:
                                is_split = False
                                try:
                                    url = f"https://api.polygon.io/v3/reference/splits?ticker={ticker}&execution_date.gte={prev_date_str}&execution_date.lte={date_str}&apiKey={self.api_key}"
                                    response = requests.get(url)
                                    if response.status_code == 200:
                                        data = response.json()
                                        is_split = 'results' in data and len(data['results']) > 0
                                        if is_split:
                                            print(f"  -> SPLIT DETECTED for {ticker}, skipping")
                                except Exception:
                                    pass
                                
                                if is_split:
                                    continue
                            
                            # Check criteria (EXACT same as your backtest)
                            if gap_928 >= 50 and price_928 >= 0.30:
                                pre_market_data = df[df['t'].dt.time < time(9, 30)]
                                pre_market_volume = pre_market_data['v'].sum() if not pre_market_data.empty else 0
                                
                                print(f"  Pre-market volume: {pre_market_volume:,}")
                                
                                if pre_market_volume >= 1000000:
                                    print(f"  -> ✓ QUALIFIED GAPPER: {ticker} - {gap_928:.1f}%")
                                    
                                    # Get day's OHLC from daily data
                                    day_data = current_data.get('results', [])
                                    daily_ohlc = next((s for s in day_data if s['T'] == ticker), {})
                                    
                                    final_candidates.append({
                                        'ticker': ticker,
                                        'gap_percentage': gap_928,
                                        'volume': pre_market_volume,
                                        'price': price_928,
                                        'previous_close': candidate['previous_close'],
                                        'gap_928': gap_928,
                                        'price_928': price_928,
                                        'date': date_str,
                                        'open': daily_ohlc.get('o', price_928),
                                        'high': daily_ohlc.get('h', price_928),
                                        'low': daily_ohlc.get('l', price_928),
                                        'close': daily_ohlc.get('c', price_928)
                                    })
                                else:
                                    print(f"  -> Volume too low: {pre_market_volume:,}")
                            else:
                                print(f"  -> Gap/price criteria not met: {gap_928:.1f}% gap, ${price_928:.4f} price")
                        else:
                            print(f"  -> No 9:28 candle found for {ticker}")
                
                # Rate limiting for premium API
                time_module.sleep(0.12)
            
            print(f"\nFinal result: {len(final_candidates)} qualified gappers")
            return final_candidates
            
        except Exception as e:
            print(f"Error fetching candidates: {str(e)}")
            import traceback
            traceback.print_exc()
            return []
    
    def get_trading_days(self, days=250):
        """Get recent trading days (default ~1 year)"""
        trading_days = []
        current_date = datetime.now(self.eastern)
        
        # Find recent trading days
        while len(trading_days) < days:
            if current_date.weekday() < 5:  # Monday = 0, Friday = 4
                trading_days.append(current_date)
            current_date -= timedelta(days=1)
        
        return list(reversed(trading_days))
    
    def fetch_intraday_chart_data(self, ticker, date_str):
        """Fetch 15-minute data for candlestick charts"""
        try:
            url = f"{self.polygon_base_url}/aggs/ticker/{ticker}/range/15/minute/{date_str}/{date_str}?adjusted=false&sort=asc&apiKey={self.api_key}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            if 'results' in data and data['results']:
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
            print(f"Error fetching chart data for {ticker}: {e}")
            
        return []
    
    def daily_update(self):
        """Main update function"""
        print(f"🚀 Starting Gap Scanner Update at {datetime.now()}")
        
        # Test API connection first
        try:
            test_url = "https://api.polygon.io/v1/marketstatus/now"
            test_response = requests.get(test_url, params={'apiKey': self.api_key}, timeout=10)
            test_response.raise_for_status()
            print("✓ API connection successful!")
        except Exception as e:
            print(f"❌ API connection failed: {e}")
            return
        
        # Get recent trading days
        trading_days = self.get_trading_days(250)  # ~1 year of trading days
        print(f"Processing {len(trading_days)} trading days (~1 year)...")
        
        all_gappers = []
        daily_counts = []
        
        # Process each trading day
        for i, date in enumerate(trading_days):
            print(f"\n{'='*60}")
            print(f"Day {i+1}/{len(trading_days)}: {date.strftime('%Y-%m-%d')}")
            
            candidates = self.fetch_candidates_for_date(date)
            
            if candidates:
                print(f"✓ Found {len(candidates)} gappers on {date.strftime('%Y-%m-%d')}")
                all_gappers.extend(candidates)
                daily_counts.append({
                    'date': date.strftime('%Y-%m-%d'),
                    'count': len(candidates)
                })
            else:
                print(f"No gappers found on {date.strftime('%Y-%m-%d')}")
                daily_counts.append({
                    'date': date.strftime('%Y-%m-%d'),
                    'count': 0
                })
        
        # Prepare cache data
        cache_data = {
            'lastUpdated': datetime.now().isoformat(),
            'gappers': {},
            'stats': {},
            'lastGaps': [],
            'intradayData': {}
        }
        
        # Get today's/latest gappers for the "Last Gaps" section
        if all_gappers:
            # Sort all gappers by date (newest first) and gap %
            all_gappers.sort(key=lambda x: (x['date'], x['gap_percentage']), reverse=True)
            
            # Get the most recent gappers (from latest trading day with gappers)
            latest_date = all_gappers[0]['date']
            latest_gappers = [g for g in all_gappers if g['date'] == latest_date]
            
            cache_data['lastGaps'] = [{
                'ticker': g['ticker'],
                'gapPercentage': g['gap_percentage'],
                'volume': g['volume'],
                'date': g['date']
            } for g in latest_gappers[:20]]  # Top 20 from latest day
            
            print(f"\n✓ Using {len(latest_gappers)} gappers from {latest_date} as 'Today's Gappers'")
        
        # Group gappers by date for the gappers section
        gappers_by_date = {}
        for gapper in all_gappers:
            date = gapper['date']
            if date not in gappers_by_date:
                gappers_by_date[date] = []
            gappers_by_date[date].append(gapper)
        
        cache_data['gappers'] = gappers_by_date
        
        # Calculate statistics
        if all_gappers:
            avg_gaps_per_day = len(all_gappers) / len(trading_days)
            
            # Sort by gap percentage for biggest gappers
            sorted_gappers = sorted(all_gappers, key=lambda x: x['gap_percentage'], reverse=True)
            
            cache_data['stats'] = {
                'averageGapsPerDay': round(avg_gaps_per_day, 2),
                'totalGappers': len(all_gappers),
                'biggestGappers': sorted_gappers[:100],
                'dailyCounts': daily_counts
            }
        else:
            cache_data['stats'] = {
                'averageGapsPerDay': 0,
                'totalGappers': 0,
                'biggestGappers': [],
                'dailyCounts': daily_counts
            }
        
        # Fetch intraday chart data for top recent gappers
        if cache_data['lastGaps']:
            print(f"\nFetching intraday chart data for top gappers...")
            for i, gapper in enumerate(cache_data['lastGaps'][:3]):  # Top 3
                ticker = gapper['ticker']
                date = gapper['date']
                print(f"Fetching chart data for {ticker} on {date}...")
                
                chart_data = self.fetch_intraday_chart_data(ticker, date)
                if chart_data:
                    cache_data['intradayData'][ticker] = {
                        'data': chart_data,
                        'date': date,
                        'gapPercentage': gapper['gapPercentage']
                    }
                
                time_module.sleep(0.2)  # Rate limit
        
        # Save to cache file
        with open(self.cache_file, 'w') as f:
            json.dump(cache_data, f, indent=2)
            
        print(f"\n✅ UPDATE COMPLETE!")
        print(f"📁 Results saved to: {self.cache_file}")
        print(f"📊 Total gappers found: {len(all_gappers)}")
        print(f"📈 Latest day gappers: {len(cache_data['lastGaps'])}")
        
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
