#!/usr/bin/env python3
"""
FIXED Gap Scanner Data Updater - Corrected Data Scale for HOD/LOD Intersection
Key Fixes:
1. Individual chart data now uses percentages from market open (not dollar prices)
2. Both averaging and individual data use same intelligent price selection logic
3. Mathematical consistency ensures blue price curve can reach green/red HOD/LOD lines
4. Enhanced verification output to confirm intersections work
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
        self.cache_file = 'gap_data_cache.json'  # Save directly in root
        
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
        """
        FIXED: Now both averaging data AND individual chart data use same percentage scale
        This ensures the blue price curve can actually reach the green/red HOD/LOD lines
        """
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
            
            # Get pre-market volume (before 9:30 AM) for qualification only
            pre_market = df[df['t'].dt.time < time(9, 30)]
            pre_market_volume = pre_market['v'].sum() if not pre_market.empty else 0
            
            # STRICT MARKET HOURS ONLY (9:30 AM - 4:00 PM EST)
            market_hours = df[
                (df['t'].dt.time >= time(9, 30)) & 
                (df['t'].dt.time <= time(16, 0))
            ].copy()
            
            if market_hours.empty or pre_market_volume < 1000000:
                return None
            
            # Get day's OHLC from MARKET HOURS ONLY
            day_open = market_hours['o'].iloc[0]  # First market hour open (9:30 AM)
            day_high = market_hours['h'].max()    # HOD from 9:30-4:00 ONLY
            day_low = market_hours['l'].min()     # LOD from 9:30-4:00 ONLY  
            day_close = market_hours['c'].iloc[-1]
            
            # Find WHEN the HOD and LOD occurred
            hod_row = market_hours[market_hours['h'] == day_high].iloc[0]
            hod_time = hod_row['t']
            lod_row = market_hours[market_hours['l'] == day_low].iloc[0]
            lod_time = lod_row['t']
            
            # Calculate HOD time as percentage of trading day (0 = 9:30, 1 = 4:00)
            market_start = hod_time.replace(hour=9, minute=30, second=0, microsecond=0)
            market_end = hod_time.replace(hour=16, minute=0, second=0, microsecond=0)
            total_market_seconds = (market_end - market_start).total_seconds()
            hod_seconds_from_start = (hod_time - market_start).total_seconds()
            hod_time_percentage = max(0, min(1, hod_seconds_from_start / total_market_seconds))
            
            # Validate gap percentage vs actual opening
            actual_gap = ((day_open - prev_close) / prev_close) * 100
            if actual_gap < 50:  # Must be 50%+ gap
                return None
            
            # Create 5-minute intervals for better resolution
            market_hours.set_index('t', inplace=True)
            resampled = market_hours.resample('5min').agg({
                'o': 'first',
                'h': 'max', 
                'l': 'min',
                'c': 'last',
                'v': 'sum'
            }).dropna()
            
            if resampled.empty:
                return None
            
            # Pre-calculate daily extremes as percentages for comparison
            daily_high_pct = ((day_high - day_open) / day_open) * 100
            daily_low_pct = ((day_low - day_open) / day_open) * 100
            
            # Create normalized time series with INTELLIGENT PRICE CURVE
            times_normalized = []
            prices_normalized = []
            highs_normalized = []
            lows_normalized = []
            
            # FIXED: Also create individual chart data using the SAME logic
            individual_time_labels = []
            individual_price_values_pct = []  # Now in percentages, not dollars!
            
            for i, (timestamp, row) in enumerate(resampled.iterrows()):
                # Calculate progress through trading day
                seconds_from_930 = (timestamp.replace(tzinfo=eastern) - market_start).total_seconds()
                progress = max(0, min(1, seconds_from_930 / total_market_seconds))
                times_normalized.append(progress)
                
                # Calculate all possible price representations for this 5-min interval
                interval_high_pct = ((row['h'] - day_open) / day_open) * 100
                interval_low_pct = ((row['l'] - day_open) / day_open) * 100
                interval_close_pct = ((row['c'] - day_open) / day_open) * 100
                interval_open_pct = ((row['o'] - day_open) / day_open) * 100
                interval_midpoint_pct = (interval_high_pct + interval_low_pct) / 2
                
                # INTELLIGENT PRICE SELECTION for gap analysis:
                
                # Check if this interval contains the daily extremes (within 1% tolerance)
                contains_daily_high = abs(interval_high_pct - daily_high_pct) < 1.0
                contains_daily_low = abs(interval_low_pct - daily_low_pct) < 1.0
                
                if contains_daily_high:
                    # This interval has the HOD - use it! (Critical for gap traders)
                    price_pct = interval_high_pct
                    
                elif contains_daily_low:
                    # This interval has the LOD - use it! (Critical for gap traders)
                    price_pct = interval_low_pct
                    
                elif abs(interval_high_pct) > abs(interval_low_pct) and abs(interval_high_pct) > 3:
                    # Significant upward move (>3%) - use the high (gap traders care about momentum)
                    price_pct = interval_high_pct
                    
                elif abs(interval_low_pct) > 3:
                    # Significant downward move (>3%) - use the low (gap traders care about fades)
                    price_pct = interval_low_pct
                    
                elif abs(interval_high_pct - interval_low_pct) > 5:
                    # High volatility interval (>5% range) - use the more extreme point
                    price_pct = interval_high_pct if abs(interval_high_pct) > abs(interval_low_pct) else interval_low_pct
                    
                else:
                    # Normal trading period - use weighted blend of close and midpoint
                    # Close gets 70% weight (where it settled), midpoint gets 30% (trading range)
                    price_pct = (interval_close_pct * 0.7) + (interval_midpoint_pct * 0.3)
                
                prices_normalized.append(price_pct)
                highs_normalized.append(interval_high_pct)
                lows_normalized.append(interval_low_pct)
                
                # FIXED: Individual chart data now uses the SAME intelligent selection!
                individual_time_labels.append(timestamp.strftime('%H:%M'))
                individual_price_values_pct.append(price_pct)  # Same logic as averaging!
            
            # VERIFICATION: Check that our price curve can reach the extremes
            max_price_curve = max(prices_normalized) if prices_normalized else 0
            min_price_curve = min(prices_normalized) if prices_normalized else 0
            
            # Debug output to verify the fix works
            print(f"  FIXED {ticker}: Price curve range: {min_price_curve:.1f}% to {max_price_curve:.1f}%")
            print(f"  FIXED {ticker}: HOD/LOD targets: {daily_high_pct:.1f}% / {daily_low_pct:.1f}%")
            print(f"  FIXED {ticker}: Can reach HOD: {max_price_curve >= daily_high_pct * 0.9}")
            print(f"  FIXED {ticker}: Can reach LOD: {min_price_curve <= daily_low_pct * 0.9}")
            print(f"  FIXED {ticker}: Individual chart data range: {min(individual_price_values_pct):.1f}% to {max(individual_price_values_pct):.1f}%")
            
            # Calculate stats - ALL based on MARKET HOURS data using market open as baseline
            open_to_close_change = ((day_close - day_open) / day_open) * 100
            high_of_day_pct = daily_high_pct  # HOD % from market open
            low_of_day_pct = daily_low_pct     # LOD % from market open
            total_volume = int(market_hours['v'].sum())
            dollar_volume = total_volume * day_open
            
            return {
                'ticker': ticker,
                'date': date_str,
                'gap_percentage': float(actual_gap),
                'previous_close': float(prev_close),
                'open': float(day_open),
                'high': float(day_high),
                'low': float(day_low),
                'close': float(day_close),
                'open_to_close_change': float(open_to_close_change),
                'high_of_day_pct': float(high_of_day_pct),  # HOD as % from market open
                'low_of_day_pct': float(low_of_day_pct),    # LOD as % from market open
                'hod_time_percentage': float(hod_time_percentage),
                'hod_time_str': hod_time.strftime('%H:%M'),
                'total_volume': total_volume,
                'dollar_volume': int(dollar_volume),
                'pre_market_volume': int(pre_market_volume),
                'times_normalized': times_normalized,
                'prices_normalized': prices_normalized,      # For averaging - can reach HOD/LOD!
                'highs_normalized': highs_normalized,
                'lows_normalized': lows_normalized,
                # FIXED: Individual chart data now in percentages and uses same logic!
                'time_labels': individual_time_labels,
                'price_values': individual_price_values_pct  # NOW IN PERCENTAGES!
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
                        print(f"  ✓ Qualified: {ticker} - Gap: {gapper_data['gap_percentage']:.1f}%, O-to-C: {gapper_data['open_to_close_change']:.1f}%, HOD: {gapper_data['hod_time_str']}")
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
        """
        Calculate average pattern with 5-minute resolution
        Now works with the fixed price curves that can reach HOD/LOD
        """
        if not gappers:
            return None
            
        # Create standardized time points for 5-minute intervals (78 points for 390 minutes)
        market_minutes = 6.5 * 60  # 390 minutes
        time_points = np.linspace(0, 1, 78)  # 78 points for 5-min intervals
        
        all_price_curves = []
        all_high_curves = []
        all_low_curves = []
        
        total_volume = 0
        total_dollar_volume = 0
        total_gap = 0
        total_otc = 0
        high_of_day_percentages = []
        high_of_day_times = []
        low_of_day_percentages = []
        
        for gapper in gappers:
            if len(gapper['times_normalized']) > 1:
                # Interpolate each gapper's data to standard time points
                price_interp = np.interp(time_points, gapper['times_normalized'], gapper['prices_normalized'])
                high_interp = np.interp(time_points, gapper['times_normalized'], gapper['highs_normalized'])
                low_interp = np.interp(time_points, gapper['times_normalized'], gapper['lows_normalized'])
                
                all_price_curves.append(price_interp)
                all_high_curves.append(high_interp)
                all_low_curves.append(low_interp)
                
                # Track HOD timing from the processed data
                high_of_day_times.append(gapper['hod_time_percentage'])
            
            # Accumulate totals using MARKET HOURS data
            total_volume += gapper['total_volume']
            total_dollar_volume += gapper['dollar_volume']
            total_gap += gapper['gap_percentage']
            total_otc += gapper['open_to_close_change']
            high_of_day_percentages.append(gapper['high_of_day_pct'])
            low_of_day_percentages.append(gapper['low_of_day_pct'])
        
        if not all_price_curves:
            return None
            
        # Calculate averages - NOW they should intersect with HOD/LOD!
        avg_prices = np.mean(all_price_curves, axis=0)
        avg_highs = np.mean(all_high_curves, axis=0)
        avg_lows = np.mean(all_low_curves, axis=0)
        
        # Calculate average HOD/LOD percentages from MARKET OPEN
        avg_high_of_day_pct = np.mean(high_of_day_percentages)  # Average % gain to HOD
        avg_low_of_day_pct = np.mean(low_of_day_percentages)    # Average % loss to LOD
        avg_hod_time = np.mean(high_of_day_times)               # Average time HOD occurs
        
        # VERIFICATION: Check intersection capability
        max_avg_price = np.max(avg_prices)
        min_avg_price = np.min(avg_prices)
        
        print(f"PERIOD {period_name} VERIFICATION:")
        print(f"  Avg price curve range: {min_avg_price:.1f}% to {max_avg_price:.1f}%")
        print(f"  Target HOD/LOD: {avg_high_of_day_pct:.1f}% / {avg_low_of_day_pct:.1f}%")
        print(f"  Intersection ratio: {(max_avg_price/avg_high_of_day_pct)*100:.0f}% HOD, {(min_avg_price/avg_low_of_day_pct)*100:.0f}% LOD")
        
        # Convert average HOD time back to actual time
        hod_minutes_from_930 = avg_hod_time * market_minutes
        hod_hour = 9 + int(hod_minutes_from_930 // 60)
        hod_minute = 30 + int(hod_minutes_from_930 % 60)
        if hod_minute >= 60:
            hod_hour += 1
            hod_minute -= 60
        avg_hod_time_str = f"{hod_hour:02d}:{hod_minute:02d}"
        
        # Create time labels for 5-minute intervals (9:30 AM - 4:00 PM)
        time_labels = []
        for i, t in enumerate(time_points):
            minutes_from_930 = t * market_minutes
            hour = 9 + int(minutes_from_930 // 60)
            minute = 30 + int(minutes_from_930 % 60)
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
            'avg_high_of_day_pct': round(avg_high_of_day_pct, 2),     # GREEN LINE
            'avg_low_of_day_pct': round(avg_low_of_day_pct, 2),       # RED LINE
            'avg_hod_time': avg_hod_time,                             # YELLOW LINE position
            'avg_hod_time_str': avg_hod_time_str,
            'time_labels': time_labels,
            'avg_prices': [round(p, 2) for p in avg_prices],          # BLUE LINE - NOW intersects!
            'avg_highs': [round(h, 2) for h in avg_highs],
            'avg_lows': [round(l, 2) for l in avg_lows],
            'open_line': [0.0] * len(time_labels)  # Reference line at 0% (market open)
        }
    
    def calculate_all_period_averages(self, all_gappers):
        """Calculate monthly, weekly, and daily averages with proper 12-month range"""
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
        
        # Calculate monthly averages - proper 12 months back from current date
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        
        # Start from current date and go back 12 months
        current_date = datetime.now()
        monthly_averages = {}
        
        # Generate the last 12 months from current date
        for i in range(11, -1, -1):  # 11 months back to current month
            target_date = current_date.replace(day=1) - timedelta(days=i*30)  # Approximate month subtraction
            target_date = target_date.replace(day=1)  # Ensure we're at start of month
            month_key = f"{target_date.year}-{target_date.month:02d}"
            
            if month_key in monthly_data and monthly_data[month_key]:
                gappers = monthly_data[month_key]
                month_name = month_names[target_date.month - 1]
                period_avg = self.calculate_period_average(gappers, f"{month_name} {target_date.year}")
                if period_avg:
                    period_avg.update({
                        'month': month_name,
                        'year': target_date.year,
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
        for month_key, data in sorted_months:
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
        for week_key, data in sorted_weeks:
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
        for daily_key, data in sorted_days:
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
    
    def get_trading_days(self, days=250):
        """Get recent trading days - increased to 250 days to ensure 12 full months"""
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
        """Main update function with fixed data scale implementation"""
        print(f"🚀 Starting FIXED Gap Scanner Update at {datetime.now()}")
        print("✅ CRITICAL FIX: Individual chart data now uses percentages from market open")
        print("✅ CRITICAL FIX: Both averaging and individual data use same intelligent selection")
        print("✅ CRITICAL FIX: Blue price curve can now reach green/red HOD/LOD lines")
        print("🔧 Features:")
        print("   - 5-minute intervals for better resolution (78 data points)")
        print("   - Intelligent price selection prioritizing extremes")
        print("   - Mathematical consistency between all chart elements")
        print("   - Market hours only calculations (9:30-4:00 PM EST)")
        print("   - Enhanced verification system")
        
        # Test API connection
        try:
            test_url = "https://api.polygon.io/v1/marketstatus/now"
            test_response = requests.get(test_url, params={'apiKey': self.api_key}, timeout=10)
            test_response.raise_for_status()
            print("✓ API connection successful!")
        except Exception as e:
            print(f"❌ API connection failed: {e}")
            return
        
        # Get trading days to ensure 12 full months
        trading_days = self.get_trading_days(250)  # 250 days for 12+ months
        print(f"Processing {len(trading_days)} trading days to ensure 12 full months...")
        
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
                    'price_values': g['price_values'],  # NOW IN PERCENTAGES!
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
            
        print(f"\n✅ FIXED UPDATE COMPLETE!")
        print(f"📁 Results saved to: {self.cache_file}")
        print(f"📊 Total gappers processed: {len(all_gappers)}")
        print(f"📅 Monthly averages: {len(monthly_averages)} months")
        print(f"📅 Weekly averages: {len(weekly_averages)} weeks")  
        print(f"📅 Daily averages: {len(daily_averages)} days")
        print(f"🗓️ Days since last gap: {calendar_data['days_since_last_gap']}")
        
        # Show date range covered
        if monthly_averages:
            month_keys = sorted(monthly_averages.keys())
            print(f"📆 Month range: {month_keys[0]} to {month_keys[-1]}")
        
        # Sample verification info
        if monthly_averages:
            sample_month = list(monthly_averages.values())[0]
            print(f"📈 Look for VERIFICATION output above showing intersection ratios")
            print(f"📊 Using 5-minute intervals: {len(sample_month.get('time_labels', []))} data points per chart")
        
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
