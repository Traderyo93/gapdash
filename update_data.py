#!/usr/bin/env python3
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
        self.cache_file = 'gap_data_cache.json'
        
        os.makedirs(self.data_dir, exist_ok=True)
        self.polygon_base_url = "https://api.polygon.io/v2"
        
    def filter_ticker_symbols(self, ticker):
        invalid_suffixes = ('WS', 'RT', 'WSA')
        
        if len(ticker) >= 5:
            return False
        
        if any(ticker.endswith(suffix) for suffix in invalid_suffixes):
            return False
        
        if ticker in ['ZVZZT', 'ZWZZT', 'ZBZZT']:
            return False
        
        return True
    
    def get_previous_trading_day(self, date):
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
            
            pre_market = df[df['t'].dt.time < time(9, 30)]
            pre_market_volume = pre_market['v'].sum() if not pre_market.empty else 0
            
            market_hours = df[
                (df['t'].dt.time >= time(9, 30)) & 
                (df['t'].dt.time <= time(16, 0))
            ].copy()
            
            if market_hours.empty or pre_market_volume < 1000000:
                return None
            
            day_open = market_hours['o'].iloc[0]
            day_high = market_hours['h'].max()
            day_low = market_hours['l'].min()
            day_close = market_hours['c'].iloc[-1]
            
            hod_row = market_hours[market_hours['h'] == day_high].iloc[0]
            hod_time = hod_row['t']
            lod_row = market_hours[market_hours['l'] == day_low].iloc[0]
            lod_time = lod_row['t']
            
            market_start = hod_time.replace(hour=9, minute=30, second=0, microsecond=0)
            market_end = hod_time.replace(hour=16, minute=0, second=0, microsecond=0)
            total_market_seconds = (market_end - market_start).total_seconds()
            hod_seconds_from_start = (hod_time - market_start).total_seconds()
            hod_time_percentage = max(0, min(1, hod_seconds_from_start / total_market_seconds))
            
            actual_gap = ((day_open - prev_close) / prev_close) * 100
            if actual_gap < 50:
                return None
            
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
            
            daily_high_pct = ((day_high - day_open) / day_open) * 100
            daily_low_pct = ((day_low - day_open) / day_open) * 100
            
            times_normalized = [0.0]
            prices_normalized = [0.0]
            highs_normalized = [0.0]
            lows_normalized = [0.0]
            
            individual_time_labels = ['09:30']
            individual_price_values_pct = [0.0]
            
            for i, (timestamp, row) in enumerate(resampled.iterrows()):
                seconds_from_930 = (timestamp.replace(tzinfo=eastern) - market_start).total_seconds()
                progress = max(0, min(1, seconds_from_930 / total_market_seconds))
                times_normalized.append(progress)
                
                interval_high_pct = ((row['h'] - day_open) / day_open) * 100
                interval_low_pct = ((row['l'] - day_open) / day_open) * 100
                interval_close_pct = ((row['c'] - day_open) / day_open) * 100
                interval_open_pct = ((row['o'] - day_open) / day_open) * 100
                interval_midpoint_pct = (interval_high_pct + interval_low_pct) / 2
                
                contains_daily_high = abs(interval_high_pct - daily_high_pct) < 1.0
                contains_daily_low = abs(interval_low_pct - daily_low_pct) < 1.0
                
                if contains_daily_high:
                    price_pct = interval_high_pct
                elif contains_daily_low:
                    price_pct = interval_low_pct
                elif abs(interval_high_pct) > abs(interval_low_pct) and abs(interval_high_pct) > 3:
                    price_pct = interval_high_pct
                elif abs(interval_low_pct) > 3:
                    price_pct = interval_low_pct
                elif abs(interval_high_pct - interval_low_pct) > 5:
                    price_pct = interval_high_pct if abs(interval_high_pct) > abs(interval_low_pct) else interval_low_pct
                else:
                    price_pct = (interval_close_pct * 0.7) + (interval_midpoint_pct * 0.3)
                
                prices_normalized.append(price_pct)
                highs_normalized.append(interval_high_pct)
                lows_normalized.append(interval_low_pct)
                
                individual_time_labels.append(timestamp.strftime('%H:%M'))
                individual_price_values_pct.append(price_pct)
            
            open_to_close_change = ((day_close - day_open) / day_open) * 100
            high_of_day_pct = daily_high_pct
            low_of_day_pct = daily_low_pct
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
                'high_of_day_pct': float(high_of_day_pct),
                'low_of_day_pct': float(low_of_day_pct),
                'hod_time_percentage': float(hod_time_percentage),
                'hod_time_str': hod_time.strftime('%H:%M'),
                'total_volume': total_volume,
                'dollar_volume': int(dollar_volume),
                'pre_market_volume': int(pre_market_volume),
                'times_normalized': times_normalized,
                'prices_normalized': prices_normalized,
                'highs_normalized': highs_normalized,
                'lows_normalized': lows_normalized,
                'time_labels': individual_time_labels,
                'price_values': individual_price_values_pct
            }
            
        except Exception as e:
            print(f"Error processing intraday data for {ticker}: {e}")
            return None
            
    def fetch_candidates_for_date(self, date):
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
            
            prev_close_url = f"{self.polygon_base_url}/aggs/grouped/locale/us/market/stocks/{prev_date_str}?adjusted=false&type=CS,PS,ADR&apiKey={self.api_key}"
            prev_close_response = requests.get(prev_close_url)
            prev_close_response.raise_for_status()
            prev_close_data = prev_close_response.json()
            
            prev_closes = {stock['T']: stock['c'] for stock in prev_close_data.get('results', [])}
            
            current_url = f"{self.polygon_base_url}/aggs/grouped/locale/us/market/stocks/{date_str}?adjusted=false&type=CS,PS,ADR&apiKey={self.api_key}"
            current_response = requests.get(current_url)
            current_response.raise_for_status()
            current_data = current_response.json()
            
            initial_candidates = []
            
            if 'results' in current_data:
                for stock in current_data['results']:
                    ticker = stock['T']
                    opening = stock['o']
                    
                    if not self.filter_ticker_symbols(ticker):
                        continue
                        
                    if ticker in prev_closes:
                        prev_close = prev_closes[ticker]
                        initial_gap = ((opening - prev_close) / prev_close) * 100
                        
                        if initial_gap >= 50 and opening >= 0.30:
                            initial_candidates.append({
                                'ticker': ticker,
                                'previous_close': prev_close,
                                'initial_gap': initial_gap,
                                'opening': opening
                            })
            
            print(f"Found {len(initial_candidates)} potential gappers")
            
            qualified_gappers = []
            for i, candidate in enumerate(initial_candidates):
                ticker = candidate['ticker']
                print(f"Processing {i+1}/{len(initial_candidates)}: {ticker}")
                
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
                
                time_module.sleep(0.12)
            
            print(f"Final result: {len(qualified_gappers)} qualified gappers")
            return qualified_gappers
            
        except Exception as e:
            print(f"Error processing date {date_str}: {str(e)}")
            import traceback
            traceback.print_exc()
            return []
    
    def calculate_period_average(self, gappers, period_name):
        if not gappers:
            return None
            
        market_minutes = 6.5 * 60
        time_points = np.linspace(0, 1, 79)
        
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
                price_interp = np.interp(time_points, gapper['times_normalized'], gapper['prices_normalized'])
                high_interp = np.interp(time_points, gapper['times_normalized'], gapper['highs_normalized'])
                low_interp = np.interp(time_points, gapper['times_normalized'], gapper['lows_normalized'])
                
                all_price_curves.append(price_interp)
                all_high_curves.append(high_interp)
                all_low_curves.append(low_interp)
                
                high_of_day_times.append(gapper['hod_time_percentage'])
            
            total_volume += gapper['total_volume']
            total_dollar_volume += gapper['dollar_volume']
            total_gap += gapper['gap_percentage']
            total_otc += gapper['open_to_close_change']
            high_of_day_percentages.append(gapper['high_of_day_pct'])
            low_of_day_percentages.append(gapper['low_of_day_pct'])
        
        if not all_price_curves:
            return None
            
        avg_prices = np.mean(all_price_curves, axis=0)
        avg_highs = np.mean(all_high_curves, axis=0)
        avg_lows = np.mean(all_low_curves, axis=0)
        
        avg_high_of_day_pct = np.mean(high_of_day_percentages)
        avg_low_of_day_pct = np.mean(low_of_day_percentages)
        avg_hod_time = np.mean(high_of_day_times)
        
        hod_minutes_from_930 = avg_hod_time * market_minutes
        hod_hour = 9 + int(hod_minutes_from_930 // 60)
        hod_minute = 30 + int(hod_minutes_from_930 % 60)
        if hod_minute >= 60:
            hod_hour += 1
            hod_minute -= 60
        avg_hod_time_str = f"{hod_hour:02d}:{hod_minute:02d}"
        
        time_labels = ['09:30']
        for i, t in enumerate(time_points[1:], 1):
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
            'avg_high_of_day_pct': round(avg_high_of_day_pct, 2),
            'avg_low_of_day_pct': round(avg_low_of_day_pct, 2),
            'avg_hod_time': avg_hod_time,
            'avg_hod_time_str': avg_hod_time_str,
            'time_labels': time_labels,
            'avg_prices': [round(p, 2) for p in avg_prices],
            'avg_highs': [round(h, 2) for h in avg_highs],
            'avg_lows': [round(l, 2) for l in avg_lows],
            'open_line': [0.0] * len(time_labels)
        }
    
    def calculate_all_period_averages(self, all_gappers):
        monthly_data = defaultdict(list)
        weekly_data = defaultdict(list)
        daily_data = defaultdict(list)
        
        for gapper in all_gappers:
            date = datetime.strptime(gapper['date'], '%Y-%m-%d')
            
            month_key = f"{date.year}-{date.month:02d}"
            monthly_data[month_key].append(gapper)
            
            year, week, _ = date.isocalendar()
            week_key = f"{year}-W{week:02d}"
            weekly_data[week_key].append(gapper)
            
            daily_key = gapper['date']
            daily_data[daily_key].append(gapper)
        
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        
        current_date = datetime.now()
        monthly_averages = {}
        
        for i in range(11, -1, -1):
            target_date = current_date.replace(day=1) - timedelta(days=i*30)
            target_date = target_date.replace(day=1)
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
        trading_days = []
        current_date = datetime.now(self.eastern)
        
        while len(trading_days) < days:
            if current_date.weekday() < 5:
                trading_days.append(current_date)
            current_date -= timedelta(days=1)
        
        return list(reversed(trading_days))

    def calculate_calendar_data(self, all_gappers):
        gap_dates = set()
        for gapper in all_gappers:
            gap_dates.add(gapper['date'])
        
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
        print(f"🚀 Starting Gap Scanner Update at {datetime.now()}")
        
        try:
            test_url = "https://api.polygon.io/v1/marketstatus/now"
            test_response = requests.get(test_url, params={'apiKey': self.api_key}, timeout=10)
            test_response.raise_for_status()
            print("✓ API connection successful!")
        except Exception as e:
            print(f"❌ API connection failed: {e}")
            return
        
        trading_days = self.get_trading_days(250)
        print(f"Processing {len(trading_days)} trading days...")
        
        all_gappers = []
        
        for i, date in enumerate(trading_days):
            print(f"\nDay {i+1}/{len(trading_days)}: {date.strftime('%Y-%m-%d')}")
            
            daily_gappers = self.fetch_candidates_for_date(date)
            if daily_gappers:
                all_gappers.extend(daily_gappers)
        
        print(f"\n📊 Processing {len(all_gappers)} total gappers...")
        
        monthly_averages, weekly_averages, daily_averages = self.calculate_all_period_averages(all_gappers)
        time_aggregates = self.calculate_time_period_aggregates(monthly_averages, weekly_averages, daily_averages)
        calendar_data = self.calculate_calendar_data(all_gappers)
        
        recent_gappers = sorted(all_gappers, key=lambda x: x['date'], reverse=True)[:50]
        
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
        
        with open(self.cache_file, 'w') as f:
            json.dump(cache_data, f, indent=2)
            
        print(f"\n✅ Gap Scanner Update Complete!")
        print(f"📁 Results saved to: {self.cache_file}")
        print(f"📊 Total gappers processed: {len(all_gappers)}")
        print(f"📅 Monthly averages: {len(monthly_averages)} months")
        print(f"📅 Weekly averages: {len(weekly_averages)} weeks")  
        print(f"📅 Daily averages: {len(daily_averages)} days")
        print(f"🗓️ Days since last gap: {calendar_data['days_since_last_gap']}")
        
        if os.path.exists(self.cache_file):
            file_size = os.path.getsize(self.cache_file)
            print(f"✓ Cache file created successfully: {file_size:,} bytes")
        else:
            print("❌ ERROR: Cache file was not created!")

def main():
    updater = GapDataUpdater()
    updater.daily_update()

if __name__ == "__main__":
    main()
