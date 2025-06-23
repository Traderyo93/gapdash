#!/usr/bin/env python3
"""
Gap Scanner Data Updater - New Dashboard Format
Updates gap scanner data with monthly/weekly aggregations
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
                                        'gap_percentage': float(gap_928),
                                        'volume': int(pre_market_volume),
                                        'price': float(price_928),
                                        'previous_close': float(candidate['previous_close']),
                                        'gap_928': float(gap_928),
                                        'price_928': float(price_928),
                                        'date': date_str,
                                        'open': float(daily_ohlc.get('o', price_928)),
                                        'high': float(daily_ohlc.get('h', price_928)),
                                        'low': float(daily_ohlc.get('l', price_928)),
                                        'close': float(daily_ohlc.get('c', price_928)),
                                        'dollar_volume': int(pre_market_volume) * float(price_928)
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
    
    def get_trading_days(self, days=500):
        """Get recent trading days (2+ years for proper aggregation)"""
        trading_days = []
        current_date = datetime.now(self.eastern)
        
        while len(trading_days) < days:
            if current_date.weekday() < 5:  # Monday = 0, Friday = 4
                trading_days.append(current_date)
            current_date -= timedelta(days=1)
        
        return list(reversed(trading_days))
    
    def calculate_monthly_aggregations(self, all_gappers):
        """Calculate monthly aggregated statistics for dashboard"""
        monthly_stats = defaultdict(lambda: {
            'gappers': [],
            'total_volume': 0,
            'total_dollar_volume': 0,
            'open_to_close_changes': []
        })
        
        # Group by month
        for gapper in all_gappers:
            date = datetime.strptime(gapper['date'], '%Y-%m-%d')
            month_key = f"{date.year}-{date.month:02d}"
            
            monthly_stats[month_key]['gappers'].append(gapper)
            monthly_stats[month_key]['total_volume'] += gapper.get('volume', 0)
            monthly_stats[month_key]['total_dollar_volume'] += gapper.get('dollar_volume', 0)
            
            # Calculate open to close change
            if gapper.get('open') and gapper.get('close'):
                otc_change = ((gapper['close'] - gapper['open']) / gapper['open']) * 100
                monthly_stats[month_key]['open_to_close_changes'].append(otc_change)
        
        # Convert to final format (last 24 months)
        result = []
        now = datetime.now()
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        
        for i in range(23, -1, -1):  # Last 24 months
            target_date = datetime(now.year, now.month, 1) - timedelta(days=32*i)
            target_date = target_date.replace(day=1)
            month_key = f"{target_date.year}-{target_date.month:02d}"
            
            stats = monthly_stats.get(month_key, {
                'gappers': [],
                'total_volume': 0,
                'total_dollar_volume': 0,
                'open_to_close_changes': []
            })
            
            avg_otc = 0
            if stats['open_to_close_changes']:
                avg_otc = sum(stats['open_to_close_changes']) / len(stats['open_to_close_changes'])
            
            result.append({
                'month': month_names[target_date.month - 1],
                'year': target_date.year,
                'month_key': month_key,
                'gapper_count': len(stats['gappers']),
                'total_volume': stats['total_volume'],
                'total_dollar_volume': stats['total_dollar_volume'],
                'avg_open_to_close': round(avg_otc, 2)
            })
        
        return result
    
    def calculate_weekly_aggregations(self, all_gappers):
        """Calculate weekly aggregated statistics"""
        weekly_stats = defaultdict(lambda: {
            'gappers': [],
            'open_to_close_changes': []
        })
        
        # Group by week
        for gapper in all_gappers:
            date = datetime.strptime(gapper['date'], '%Y-%m-%d')
            year, week, _ = date.isocalendar()
            week_key = f"{year}-W{week:02d}"
            
            weekly_stats[week_key]['gappers'].append(gapper)
            
            if gapper.get('open') and gapper.get('close'):
                otc_change = ((gapper['close'] - gapper['open']) / gapper['open']) * 100
                weekly_stats[week_key]['open_to_close_changes'].append(otc_change)
        
        # Convert to final format (last 12 weeks)
        result = []
        now = datetime.now()
        
        for i in range(11, -1, -1):  # Last 12 weeks
            target_date = now - timedelta(weeks=i)
            year, week, _ = target_date.isocalendar()
            week_key = f"{year}-W{week:02d}"
            
            stats = weekly_stats.get(week_key, {
                'open_to_close_changes': []
            })
            
            avg_otc = 0
            if stats['open_to_close_changes']:
                avg_otc = sum(stats['open_to_close_changes']) / len(stats['open_to_close_changes'])
            
            result.append({
                'week_key': week_key,
                'avg_open_to_close': round(avg_otc, 2)
            })
        
        return result
    
    def calculate_calendar_data(self, all_gappers):
        """Calculate which days had gaps for calendar view"""
        gap_dates = set()
        for gapper in all_gappers:
            gap_dates.add(gapper['date'])
        
        # Calculate days since last gap
        today = datetime.now().date()
        days_since_gap = 0
        
        current_date = today
        while current_date.strftime('%Y-%m-%d') not in gap_dates and days_since_gap < 30:
            if current_date.weekday() < 5:  # Only count trading days
                days_since_gap += 1
            current_date -= timedelta(days=1)
        
        return {
            'gap_dates': list(gap_dates),
            'days_since_last_gap': days_since_gap
        }
    
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
        
        # Get recent trading days (2+ years for proper aggregation)
        trading_days = self.get_trading_days(500)
        print(f"Processing {len(trading_days)} trading days...")
        
        all_gappers = []
        
        # Process each trading day
        for i, date in enumerate(trading_days):
            print(f"\n{'='*60}")
            print(f"Day {i+1}/{len(trading_days)}: {date.strftime('%Y-%m-%d')}")
            
            candidates = self.fetch_candidates_for_date(date)
            
            if candidates:
                print(f"✓ Found {len(candidates)} gappers on {date.strftime('%Y-%m-%d')}")
                all_gappers.extend(candidates)
            else:
                print(f"No gappers found on {date.strftime('%Y-%m-%d')}")
        
        # Calculate aggregations
        print(f"\n📊 Calculating aggregations...")
        monthly_stats = self.calculate_monthly_aggregations(all_gappers)
        weekly_stats = self.calculate_weekly_aggregations(all_gappers)
        calendar_data = self.calculate_calendar_data(all_gappers)
        
        # Get last gaps (last 5 trading days)
        all_gappers_sorted = sorted(all_gappers, key=lambda x: x['date'], reverse=True)
        unique_dates = []
        seen_dates = set()
        for gapper in all_gappers_sorted:
            if gapper['date'] not in seen_dates:
                unique_dates.append(gapper['date'])
                seen_dates.add(gapper['date'])
            if len(unique_dates) >= 5:
                break
        
        last_5_dates = unique_dates[:5]
        last_gaps = [g for g in all_gappers_sorted if g['date'] in last_5_dates][:50]
        
        # Prepare cache data for new dashboard
        cache_data = {
            'lastUpdated': datetime.now().isoformat(),
            'monthlyStats': monthly_stats,
            'weeklyStats': weekly_stats,
            'calendarData': calendar_data,
            'lastGaps': [{
                'ticker': g['ticker'],
                'gapPercentage': float(g['gap_percentage']),
                'volume': int(g['volume']),
                'date': g['date']
            } for g in last_gaps],
            'totalGappers': len(all_gappers)
        }
        
        # Save to cache file
        with open(self.cache_file, 'w') as f:
            json.dump(cache_data, f, indent=2)
            
        print(f"\n✅ UPDATE COMPLETE!")
        print(f"📁 Results saved to: {self.cache_file}")
        print(f"📊 Total gappers found: {len(all_gappers)}")
        print(f"📈 Monthly data points: {len(monthly_stats)}")
        print(f"📅 Weekly data points: {len(weekly_stats)}")
        print(f"🗓️ Days since last gap: {calendar_data['days_since_last_gap']}")
        
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
