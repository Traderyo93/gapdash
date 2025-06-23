#!/usr/bin/env python3
"""
Gap Scanner Data Updater - Complete Dashboard with D/W/M Support
Calculates monthly, weekly, and daily averages AND individual gapper data
"""

import os
import sys
import json
import requests
from datetime import datetime, timedelta, time
import pytz
import pandas as pd
import numpy as np
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
        """Fetch detailed minute-by-minute data"""
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

    def process_gapper_intraday(self, intraday_data, ticker, date_str, prev_close, gap_percentage):
        """Process individual gapper's intraday data"""
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
            
            # Get pre-market volume (before 9:30 AM)
            pre_market = df[df['t'].dt.time < time(9, 30)]
            pre_market_volume = pre_market['v'].sum() if not pre_market.empty else 0
            
            # Filter to market hours ONLY (9:30 AM - 4:00 PM) - NO PRE-MARKET
            market_hours = df[
                (df['t'].dt.time >= time(9, 30)) & 
                (df['t'].dt.time <= time(16, 0))
            ].copy()
            
            if market_hours.empty or pre_market_volume < 1000000:
                return None
            
            # Get day's OHLC from MARKET HOURS ONLY
            day_open = market_hours['o'].iloc[0]
            day_high = market_hours['h'].max()  # HOD from 9:30-4:00 ONLY
            day_low = market_hours['l'].min()   # LOD from 9:30-4:00 ONLY
            day_close = market_hours['c'].iloc[-1]
            
            # Validate gap percentage vs actual opening
            actual_gap = ((day_open - prev_close) / prev_close) * 100
            if actual_gap < 50:  # Must be 50%+ gap
                return None
            
            # Create 15-minute intervals for averaging
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
            
            # Create normalized time series (0-100% of trading day)
            times_normalized = []
            prices_normalized = []
            highs_normalized = []
            lows_normalized = []
            
            for i, (timestamp, row) in enumerate(resampled.iterrows()):
                progress = i / (len(resampled) - 1) if len(resampled) > 1 else 0
                times_normalized.append(progress)
                
                # Normalize prices as percentage change from open
                price_pct = ((row['c'] - day_open) / day_open) * 100
                high_pct = ((row['h'] - day_open) / day_open) * 100
                low_pct = ((row['l'] - day_open) / day_open) * 100
                
                prices_normalized.append(price_pct)
                highs_normalized.append(high_pct)
                lows_normalized.append(low_pct)
            
            # Calculate stats
            open_to_close_change = ((day_close - day_open) / day_open) * 100
            high_of_day_pct = ((day_high - day_open) / day_open) * 100
            low_of_day_pct = ((day_low - day_open) / day_open) * 100
            total_volume = int(market_hours['v'].sum())
            dollar_volume = total_volume * day_open
            
            return {
                'ticker': ticker,
                'date': date_str,
                'gap_percentage': float(actual_gap),  # Use actual gap, not initial
                'previous_close': float(prev_close),
                'open': float(day_open),
                'high': float(day_high),
                'low': float(day_low),
                'close': float(day_close),
                'open_to_close_change': float(open_to_close_change),
                'high_of_day_pct': float(high_of_day_pct),
                'low_of_day_pct': float(low_of_day_pct),
                'total_volume': total_volume,
                'dollar_volume': int(dollar_volume),
                'pre_market_volume': int(pre_market_volume),
                'times_normalized': times_normalized,
                'prices_normalized': prices_normalized,
                'highs_normalized': highs_normalized,
                'lows_normalized': lows_normalized,
                # Create actual time labels for individual charts
                'time_labels': [timestamp.strftime('%H:%M') for timestamp, _ in resampled.iterrows()],
                'price_values': [float(row['c']) for _, row in resampled.iterrows()]
            }
            
        except Exception as e:
            print(f"Error processing intraday data for {ticker}: {e}")
            return None

    def fetch_candidates_for_date(self, date):
        """Find and process gappers for a specific date"""
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
            
            # Find initial gap candidates (50%+ gap, $0.30+ price)
            if 'results' in current_data:
                for stock in current_data['results']:
                    ticker = stock['T']
                    opening = stock['o']
                    
                    if not self.filter_ticker_symbols(ticker):
                        continue
                        
                    if ticker in prev_closes:
                        prev_close = prev_closes[ticker]
                        initial_gap = ((opening - prev_close) / prev_close) * 100
                        
                        if initial_gap >= 50 and opening >= 0.30:  # 50%+ gap, $0.30+ price
                            initial_candidates.append({
                                'ticker': ticker,
                                'previous_close': prev_close,
                                'initial_gap': initial_gap,
                                'opening': opening
                            })
            
            print(f"Found {len(initial_candidates)} potential gappers")
            
            # Process each candidate for detailed data
            qualified_gappers = []
            for i, candidate in enumerate(initial_candidates):
                ticker = candidate['ticker']
                print(f"Processing {i+1}/{len(initial_candidates)}: {ticker}")
                
                # Get detailed intraday data
                intraday_data = self.fetch_detailed_intraday_data(ticker, date_str)
                if intraday_data:
                    gapper_data = self.process_gapper_intraday(
                        intraday_data, 
                        ticker, 
                        date_str, 
                        candidate['previous_close'],
                        candidate['initial_gap']
                    )
                    
                    if gapper_data:
                        print(f"  ✓ Qualified: {ticker} - Gap: {gapper_data['gap_percentage']:.1f}%, O-to-C: {gapper_data['open_to_close_change']:.1f}%")
                        qualified_gappers.append(gapper_data)
                    else:
                        print(f"  ✗ Failed qualification: {ticker}")
                
                # Rate limiting
                time_module.sleep(0.12)
            
            print(f"Final result: {len(qualified_gappers)} qualified gappers")
            return qualified_gappers
            
        except Exception as e:
            print(f"Error processing date {date_str}: {str(e)}")
            import traceback
            traceback.print_exc()
            return []
    
    def calculate_period_average(self, gappers, period_name):
        """Calculate average pattern for a period (month/week/day)"""
        if not gappers:
            return None
            
        # Create standardized time points (0 to 1, representing trading day)
        time_points = np.linspace(0, 1, 26)  # 26 points for 15-min intervals
        
        all_price_curves = []
        all_high_curves = []
        all_low_curves = []
        
        total_volume = 0
        total_dollar_volume = 0
        total_gap = 0
        total_otc = 0
        high_of_days = []
        low_of_days = []
        
        for gapper in gappers:
            if len(gapper['times_normalized']) > 1:
                # Interpolate each gapper's data to standard time points
                price_interp = np.interp(time_points, gapper['times_normalized'], gapper['prices_normalized'])
                high_interp = np.interp(time_points, gapper['times_normalized'], gapper['highs_normalized'])
                low_interp = np.interp(time_points, gapper['times_normalized'], gapper['lows_normalized'])
                
                all_price_curves.append(price_interp)
                all_high_curves.append(high_interp)
                all_low_curves.append(low_interp)
            
            # Accumulate totals
            total_volume += gapper['total_volume']
            total_dollar_volume += gapper['dollar_volume']
            total_gap += gapper['gap_percentage']
            total_otc += gapper['open_to_close_change']
            high_of_days.append(gapper['high_of_day_pct'])
            low_of_days.append(gapper['low_of_day_pct'])
        
        if not all_price_curves:
            return None
            
        # Calculate averages
        avg_prices = np.mean(all_price_curves, axis=0)
        avg_highs = np.mean(all_high_curves, axis=0)
        avg_lows = np.mean(all_low_curves, axis=0)
        
        # Calculate static HOD and LOD (average across all gappers)
        avg_high_of_day = np.mean(high_of_days)
        avg_low_of_day = np.mean(low_of_days)
        
        # Create time labels
        time_labels = []
        for i, t in enumerate(time_points):
            hour = 9 + int(t * 6.5)  # 6.5 hours of trading
            minute = 30 + int((t * 6.5 * 60) % 60)
            if minute >= 60:
                hour += 1
                minute -= 60
            time_labels.append(f"{hour:02d}:{minute:02d}")
        
        gapper_count = len(gappers)
        
        return {
            'period_name': period_name,
            'gapper_count': gapper_count,
            'avg_gap_percentage': round(total_gap / gapper_count, 2),
            'avg_open_to_close': round(total_otc / gapper_count, 2),
            'total_volume': total_volume,
            'total_dollar_volume': total_dollar_volume,
            'avg_high_of_day': round(avg_high_of_day, 2),  # Static HOD line
            'avg_low_of_day': round(avg_low_of_day, 2),    # Static LOD line
            'time_labels': time_labels,
            'avg_prices': [round(p, 2) for p in avg_prices],
            'avg_highs': [round(h, 2) for h in avg_highs],
            'avg_lows': [round(l, 2) for l in avg_lows],
            'open_line': [0.0] * len(time_labels)  # Reference line at 0%
        }
    
    def calculate_all_period_averages(self, all_gappers):
        """Calculate monthly, weekly, and daily averages"""
        monthly_data = defaultdict(list)
        weekly_data = defaultdict(list)
        daily_data = defaultdict(list)
        
        # Group by different periods
        for gapper in all_gappers:
            date = datetime.strptime(gapper['date'], '%Y-%m-%d')
            
            # Monthly grouping
            month_key = f"{date.year}-{date.month:02d}"
            monthly_data[month_key].append(gapper)
            
            # Weekly grouping
            year, week, _ = date.isocalendar()
            week_key = f"{year}-W{week:02d}"
            weekly_data[week_key].append(gapper)
            
            # Daily grouping
            daily_key = gapper['date']
            daily_data[daily_key].append(gapper)
        
        # Calculate monthly averages
        monthly_averages = {}
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        
        for month_key, gappers in monthly_data.items():
            if gappers:
                year, month = month_key.split('-')
                month_name = month_names[int(month) - 1]
                period_avg = self.calculate_period_average(gappers, f"{month_name} {year}")
                if period_avg:
                    period_avg.update({
                        'month': month_name,
                        'year': int(year),
                        'month_key': month_key
                    })
                    monthly_averages[month_key] = period_avg
        
        # Calculate weekly averages (last 12 weeks)
        weekly_averages = {}
        now = datetime.now()
        for i in range(11, -1, -1):
            target_date = now - timedelta(weeks=i)
            year, week, _ = target_date.isocalendar()
            week_key = f"{year}-W{week:02d}"
            
            if week_key in weekly_data and weekly_data[week_key]:
                gappers = weekly_data[week_key]
                period_avg = self.calculate_period_average(gappers, f"Week {week}, {year}")
                if period_avg:
                    period_avg.update({
                        'week': week,
                        'year': year,
                        'week_key': week_key
                    })
                    weekly_averages[week_key] = period_avg
        
        # Calculate daily averages (last 12 trading days with gappers)
        daily_averages = {}
        sorted_daily_keys = sorted([k for k, v in daily_data.items() if v], reverse=True)[:12]
        
        for daily_key in sorted_daily_keys:
            gappers = daily_data[daily_key]
            date = datetime.strptime(daily_key, '%Y-%m-%d')
            day_name = date.strftime('%a %m/%d')
            period_avg = self.calculate_period_average(gappers, day_name)
            if period_avg:
                period_avg.update({
                    'date': daily_key,
                    'day_name': day_name
                })
                daily_averages[daily_key] = period_avg
        
        return monthly_averages, weekly_averages, daily_averages
    
    def calculate_time_period_aggregates(self, monthly_averages, weekly_averages, daily_averages):
        """Calculate D/W/M aggregates for top bar charts"""
        
        # Monthly stats for bar charts
        monthly_stats = []
        sorted_months = sorted(monthly_averages.items(), key=lambda x: x[0])
        for month_key, data in sorted_months[-12:]:  # Last 12 months
            monthly_stats.append({
                'month': data['month'],
                'year': data['year'],
                'month_key': month_key,
                'gapper_count': data['gapper_count'],
                'total_volume': data['total_volume'],
                'total_dollar_volume': data['total_dollar_volume'],
                'avg_open_to_close': data['avg_open_to_close']
            })
        
        # Weekly stats for bar charts
        weekly_stats = []
        sorted_weeks = sorted(weekly_averages.items(), key=lambda x: x[0])
        for week_key, data in sorted_weeks[-12:]:  # Last 12 weeks
            weekly_stats.append({
                'week': data['week'],
                'year': data['year'],
                'week_key': week_key,
                'gapper_count': data['gapper_count'],
                'total_volume': data['total_volume'],
                'total_dollar_volume': data['total_dollar_volume'],
                'avg_open_to_close': data['avg_open_to_close']
            })
        
        # Daily stats for bar charts
        daily_stats = []
        sorted_days = sorted(daily_averages.items(), key=lambda x: x[0])
        for daily_key, data in sorted_days[-12:]:  # Last 12 days
            daily_stats.append({
                'date': data['date'],
                'day_name': data['day_name'],
                'gapper_count': data['gapper_count'],
                'total_volume': data['total_volume'],
                'total_dollar_volume': data['total_dollar_volume'],
                'avg_open_to_close': data['avg_open_to_close']
            })
        
        return {
            'monthly': monthly_stats,
            'weekly': weekly_stats,
            'daily': daily_stats
        }
    
    def get_trading_days(self, days=150):
        """Get recent trading days (5 months)"""
        trading_days = []
        current_date = datetime.now(self.eastern)
        
        while len(trading_days) < days:
            if current_date.weekday() < 5:  # Monday = 0, Friday = 4
                trading_days.append(current_date)
            current_date -= timedelta(days=1)
        
        return list(reversed(trading_days))
    
    def calculate_calendar_data(self, all_gappers):
        """Calculate calendar data"""
        gap_dates = set()
        for gapper in all_gappers:
            gap_dates.add(gapper['date'])
        
        # Calculate days since last gap
        today = datetime.now().date()
        days_since_gap = 0
        
        if gap_dates:
            latest_gap = max(datetime.strptime(date, '%Y-%m-%d').date() for date in gap_dates)
            days_since_gap = (today - latest_gap).days
        
        return {
            'gap_dates': list(gap_dates),
            'days_since_last_gap': max(0, days_since_gap)
        }
    
    def daily_update(self):
        """Main update function"""
        print(f"🚀 Starting Complete Gap Scanner Update at {datetime.now()}")
        
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
        trading_days = self.get_trading_days(150)  # 5 months
        print(f"Processing {len(trading_days)} trading days...")
        
        all_gappers = []
        
        # Process each trading day
        for i, date in enumerate(trading_days):
            print(f"\nDay {i+1}/{len(trading_days)}: {date.strftime('%Y-%m-%d')}")
            
            daily_gappers = self.fetch_candidates_for_date(date)
            if daily_gappers:
                all_gappers.extend(daily_gappers)
        
        print(f"\n📊 Processing {len(all_gappers)} total gappers...")
        
        # Calculate all period averages
        monthly_averages, weekly_averages, daily_averages = self.calculate_all_period_averages(all_gappers)
        
        # Calculate time period aggregates for top charts
        time_aggregates = self.calculate_time_period_aggregates(monthly_averages, weekly_averages, daily_averages)
        
        # Calculate calendar data
        calendar_data = self.calculate_calendar_data(all_gappers)
        
        # Get recent gaps for sidebar
        recent_gappers = sorted(all_gappers, key=lambda x: x['date'], reverse=True)[:50]
        
        # Prepare complete cache data
        cache_data = {
            'lastUpdated': datetime.now().isoformat(),
            'monthlyAverages': monthly_averages,
            'weeklyAverages': weekly_averages,
            'dailyAverages': daily_averages,
            'monthlyStats': time_aggregates['monthly'],
            'weeklyStats': time_aggregates['weekly'],
            'dailyStats': time_aggregates['daily'],
            'calendarData': calendar_data,
            'lastGaps': [{
                'ticker': g['ticker'],
                'gapPercentage': g['gap_percentage'],
                'volume': g['total_volume'],
                'date': g['date'],
                'openToCloseChange': g['open_to_close_change'],
                'individualData': {
                    'time_labels': g['time_labels'],
                    'price_values': g['price_values'],
                    'open': g['open'],
                    'high': g['high'],
                    'low': g['low'],
                    'close': g['close']
                }
            } for g in recent_gappers],
            'totalGappers': len(all_gappers),
            'summaryStats': {
                'total_gappers': len(all_gappers),
                'avg_gap_percentage': round(sum(g['gap_percentage'] for g in all_gappers) / len(all_gappers), 2) if all_gappers else 0,
                'avg_open_to_close': round(sum(g['open_to_close_change'] for g in all_gappers) / len(all_gappers), 2) if all_gappers else 0,
                'days_since_last_gap': calendar_data['days_since_last_gap']
            }
        }
        
        # Save to cache file
        with open(self.cache_file, 'w') as f:
            json.dump(cache_data, f, indent=2)
            
        print(f"\n✅ UPDATE COMPLETE!")
        print(f"📁 Results saved to: {self.cache_file}")
        print(f"📊 Total gappers processed: {len(all_gappers)}")
        print(f"📅 Monthly averages: {len(monthly_averages)}")
        print(f"📅 Weekly averages: {len(weekly_averages)}")  
        print(f"📅 Daily averages: {len(daily_averages)}")
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
