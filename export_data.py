#!/usr/bin/env python3
"""
Export and clean SP (SunnyPortal) and AE (AlsoEnergy) data for a date range.

Data cleaning rules:
- If AE_GHI = 0, then -1 values in SP data should become 0
- If AE_GHI > 0, missing values (-1) in SP should be filled using nearby data (forward-fill)

Usage:
    python export_data.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD
"""

import argparse
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sys


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Export and clean SP and AE data for a date range',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python export_data.py --start-date 2024-01-01 --end-date 2024-12-31
    python export_data.py -s 2025-05-12 -e 2025-05-20
    python export_data.py -s 2024-01-01 -e 2024-12-31 --device-id 28
    python export_data.py -s 2024-01-01 -e 2024-12-31 -d 29
        """
    )
    parser.add_argument(
        '--start-date', '-s',
        type=str,
        required=True,
        help='Start date in YYYY-MM-DD format'
    )
    parser.add_argument(
        '--end-date', '-e',
        type=str,
        required=True,
        help='End date in YYYY-MM-DD format'
    )
    parser.add_argument(
        '--output-dir', '-o',
        type=str,
        default='.',
        help='Output directory for the exported CSV file (default: current directory)'
    )
    parser.add_argument(
        '--device-id', '-d',
        type=str,
        default=None,
        help='Device ID to export (e.g., 28, 29, 30). If not specified, uses the first available device.'
    )
    
    return parser.parse_args()


def validate_date(date_str):
    """Validate and parse date string."""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str}. Expected YYYY-MM-DD")


def load_sp_operating_data(start_date, end_date, base_path):
    """Load SP operating data for the date range."""
    sp_op_path = os.path.join(base_path, 'sp', 'sp_data', 'operating')
    dfs = []
    
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        filepath = os.path.join(sp_op_path, f'sp_{date_str}.csv')
        
        if os.path.exists(filepath):
            try:
                df = pd.read_csv(filepath)
                df['time'] = pd.to_datetime(df['time'])
                dfs.append(df)
            except Exception as e:
                print(f"Warning: Could not load {filepath}: {e}", file=sys.stderr)
        else:
            print(f"Warning: File not found: {filepath}", file=sys.stderr)
        
        current_date += timedelta(days=1)
    
    if not dfs:
        raise ValueError("No SP operating data files found for the specified date range")
    
    combined_df = pd.concat(dfs, ignore_index=True)
    combined_df = combined_df.sort_values('time').reset_index(drop=True)
    return combined_df


def load_sp_environmental_data(start_date, end_date, base_path):
    """Load SP environmental data for the date range."""
    sp_env_path = os.path.join(base_path, 'sp', 'sp_data', 'environmental')
    dfs = []
    
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        filepath = os.path.join(sp_env_path, f'sp_{date_str}.csv')
        
        if os.path.exists(filepath):
            try:
                df = pd.read_csv(filepath)
                df['time'] = pd.to_datetime(df['time'])
                dfs.append(df)
            except Exception as e:
                print(f"Warning: Could not load {filepath}: {e}", file=sys.stderr)
        else:
            print(f"Warning: File not found: {filepath}", file=sys.stderr)
        
        current_date += timedelta(days=1)
    
    if not dfs:
        raise ValueError("No SP environmental data files found for the specified date range")
    
    combined_df = pd.concat(dfs, ignore_index=True)
    combined_df = combined_df.sort_values('time').reset_index(drop=True)
    return combined_df


def load_ae_data(start_date, end_date, base_path):
    """Load AE data for the date range."""
    ae_path = os.path.join(base_path, 'ae', 'ae_data')
    dfs = []
    
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        filepath = os.path.join(ae_path, f'ae_{date_str}.csv')
        
        if os.path.exists(filepath):
            try:
                df = pd.read_csv(filepath)
                # Handle case where first column might be index
                if 'Time' in df.columns:
                    df['Time'] = pd.to_datetime(df['Time'])
                elif 'time' in df.columns:
                    df['time'] = pd.to_datetime(df['time'])
                    df = df.rename(columns={'time': 'Time'})
                else:
                    # Assume first column is time if unnamed
                    time_col = df.columns[0]
                    df[time_col] = pd.to_datetime(df[time_col])
                    df = df.rename(columns={time_col: 'Time'})
                
                # Standardize column names
                if 'GHI' not in df.columns:
                    raise ValueError(f"GHI column not found in {filepath}")
                
                dfs.append(df)
            except Exception as e:
                print(f"Warning: Could not load {filepath}: {e}", file=sys.stderr)
        else:
            print(f"Warning: File not found: {filepath}", file=sys.stderr)
        
        current_date += timedelta(days=1)
    
    if not dfs:
        raise ValueError("No AE data files found for the specified date range")
    
    combined_df = pd.concat(dfs, ignore_index=True)
    combined_df = combined_df.sort_values('Time').reset_index(drop=True)
    
    # Resample AE data to 5-minute intervals to match SP data
    # AE data might be 1-minute intervals
    combined_df = combined_df.set_index('Time')
    combined_df = combined_df.resample('5min').mean().reset_index()
    
    return combined_df


def clean_sp_data(sp_df, ae_ghi_values, sp_columns):
    """
    Clean SP data based on AE GHI values.
    
    Rules:
    - If AE_GHI = 0, then -1 values in SP should become 0
    - If AE_GHI > 0, missing values (-1) in SP should be filled with nearby data
    """
    sp_df_cleaned = sp_df.copy()
    
    for col in sp_columns:
        if col not in sp_df_cleaned.columns:
            continue
        
        # Find -1 values
        mask_neg_one = sp_df_cleaned[col] == -1
        
        # Rule 1: If AE_GHI = 0, set -1 to 0
        mask_ghi_zero = ae_ghi_values == 0
        mask_rule1 = mask_neg_one & mask_ghi_zero
        sp_df_cleaned.loc[mask_rule1, col] = 0
        
        # Rule 2: If AE_GHI > 0, fill -1 with nearby data using forward-fill and backward-fill
        # Recompute mask_neg_one after rule 1
        mask_neg_one_remaining = sp_df_cleaned[col] == -1
        mask_ghi_positive = ae_ghi_values > 0
        mask_rule2 = mask_neg_one_remaining & mask_ghi_positive
        
        if mask_rule2.any():
            # First, replace -1 with NaN for interpolation
            sp_df_cleaned.loc[mask_rule2, col] = np.nan
            
            # Forward fill and backward fill for nearby data
            sp_df_cleaned[col] = sp_df_cleaned[col].ffill().bfill()
            
            # If still NaN after ffill/bfill, set to 0
            sp_df_cleaned[col] = sp_df_cleaned[col].fillna(0)
    
    return sp_df_cleaned


def export_data(start_date_str, end_date_str, output_dir, base_path=None, device_id=None):
    """Main function to export and clean data."""
    # Get base path (script directory)
    if base_path is None:
        base_path = os.path.dirname(os.path.abspath(__file__))
    
    # Validate dates
    start_date = validate_date(start_date_str)
    end_date = validate_date(end_date_str)
    
    if start_date > end_date:
        raise ValueError("Start date must be before or equal to end date")
    
    print(f"Loading data from {start_date_str} to {end_date_str}...")
    
    # Load data
    print("Loading SP operating data...")
    sp_op_df = load_sp_operating_data(start_date, end_date, base_path)
    
    print("Loading SP environmental data...")
    sp_env_df = load_sp_environmental_data(start_date, end_date, base_path)
    
    print("Loading AE data...")
    ae_df = load_ae_data(start_date, end_date, base_path)
    
    # Filter to only one device
    selected_device = None
    
    if 'deviceID' in sp_op_df.columns:
        available_devices = sorted(sp_op_df['deviceID'].unique())
        if len(available_devices) > 0:
            if device_id is not None:
                # Convert device_id to appropriate type (int or str) to match data
                try:
                    # Try to match as integer first
                    device_id_int = int(device_id)
                    if device_id_int in available_devices:
                        selected_device = device_id_int
                    elif float(device_id) in available_devices:
                        selected_device = float(device_id)
                    else:
                        # Try as string
                        if str(device_id) in [str(d) for d in available_devices]:
                            selected_device = device_id
                        else:
                            raise ValueError(f"Device ID {device_id} not found in operating data. Available devices: {available_devices}")
                except (ValueError, TypeError):
                    # Try as string
                    if str(device_id) in [str(d) for d in available_devices]:
                        selected_device = device_id
                    else:
                        raise ValueError(f"Device ID {device_id} not found in operating data. Available devices: {available_devices}")
            else:
                # Use first available device if not specified
                selected_device = available_devices[0]
                print(f"No device ID specified, using first available device: {selected_device}")
                print(f"Available devices: {available_devices}")
                print(f"  (Use --device-id or -d to select a different device)")
            
            print(f"Filtering operating data to deviceID: {selected_device}")
            sp_op_df = sp_op_df[sp_op_df['deviceID'] == selected_device].copy()
            
            if len(sp_op_df) == 0:
                raise ValueError(f"No data found for deviceID {selected_device} in operating data")
        else:
            print("Warning: No deviceID found in operating data", file=sys.stderr)
    else:
        print("Warning: No deviceID column found in operating data", file=sys.stderr)
    
    if 'deviceID' in sp_env_df.columns:
        available_devices_env = sorted(sp_env_df['deviceID'].unique())
        if len(available_devices_env) > 0:
            if selected_device is not None:
                # Use the same deviceID as operating data
                if selected_device not in available_devices_env:
                    # Try to find matching device
                    device_str = str(selected_device)
                    matching_devices = [d for d in available_devices_env if str(d) == device_str]
                    if matching_devices:
                        selected_device = matching_devices[0]
                    else:
                        print(f"Warning: DeviceID {selected_device} not found in environmental data. Available: {available_devices_env}", file=sys.stderr)
                        print(f"Using first available environmental device: {available_devices_env[0]}", file=sys.stderr)
                        selected_device = available_devices_env[0]
            else:
                selected_device = available_devices_env[0]
            
            print(f"Filtering environmental data to deviceID: {selected_device}")
            sp_env_df = sp_env_df[sp_env_df['deviceID'] == selected_device].copy()
            
            if len(sp_env_df) == 0:
                raise ValueError(f"No data found for deviceID {selected_device} in environmental data")
        else:
            print("Warning: No deviceID found in environmental data", file=sys.stderr)
    else:
        print("Warning: No deviceID column found in environmental data", file=sys.stderr)
    
    # Remove duplicates based on time (keep first occurrence)
    print("Removing duplicate time records...")
    sp_op_df = sp_op_df.drop_duplicates(subset=['time'], keep='first')
    sp_env_df = sp_env_df.drop_duplicates(subset=['time'], keep='first')
    
    # Merge SP data on time
    print("Merging SP operating and environmental data...")
    sp_df = pd.merge(
        sp_op_df,
        sp_env_df,
        on='time',
        how='outer',
        suffixes=('', '_env')
    )
    
    # Remove duplicate columns (deviceID)
    if 'deviceID_env' in sp_df.columns:
        sp_df = sp_df.drop(columns=['deviceID_env'])
    
    # Remove duplicates from merged dataframe (keep first)
    sp_df = sp_df.drop_duplicates(subset=['time'], keep='first')
    
    # Remove duplicates from AE data based on time
    ae_df = ae_df.drop_duplicates(subset=['Time'], keep='first')
    
    # Merge with AE data on time
    print("Merging with AE data...")
    combined_df = pd.merge(
        sp_df,
        ae_df[['Time', 'GHI']].rename(columns={'Time': 'time'}),
        on='time',
        how='outer'
    )
    
    # Sort by time and remove any remaining duplicates
    combined_df = combined_df.sort_values('time').reset_index(drop=True)
    combined_df = combined_df.drop_duplicates(subset=['time'], keep='first')
    
    # Calculate average AC power from three phases (if available)
    if all(col in combined_df.columns for col in ['ac_power_l1', 'ac_power_l2', 'ac_power_l3']):
        combined_df['ac_power_avg'] = combined_df[['ac_power_l1', 'ac_power_l2', 'ac_power_l3']].mean(axis=1)
    elif 'ac_power_l1' in combined_df.columns:
        combined_df['ac_power_avg'] = combined_df['ac_power_l1']
    else:
        combined_df['ac_power_avg'] = 0.0
    
    # Map columns to requested field names
    # Column mapping:
    # - AE_GHI: GHI from AE data
    # - SP_ambient_temp: ambient_temp1 from SP environmental data
    # - SP_rh: ambient_rh from SP environmental data
    # - SP_ac_power: average of ac_power_l1, l2, l3 from SP operating data
    # - SP_cap_temp: inv_temp1 from SP environmental data (capacitor temperature)
    # - SP_relay_temp: inv_temp2 from SP environmental data (relay temperature)
    # - SP_internal_rh: inv_rh from SP environmental data (internal relative humidity)
    
    # Create export dataframe with only requested columns
    export_df = pd.DataFrame()
    export_df['time'] = combined_df['time']
    export_df['AE_GHI'] = combined_df['GHI'].fillna(0)
    
    # SP environmental columns
    if 'ambient_temp1' in combined_df.columns:
        export_df['SP_ambient_temp'] = combined_df['ambient_temp1']
    else:
        export_df['SP_ambient_temp'] = -1
    
    if 'ambient_rh' in combined_df.columns:
        export_df['SP_rh'] = combined_df['ambient_rh']
    else:
        export_df['SP_rh'] = -1
    
    export_df['SP_ac_power'] = combined_df['ac_power_avg']
    
    if 'inv_temp1' in combined_df.columns:
        export_df['SP_cap_temp'] = combined_df['inv_temp1']
    else:
        export_df['SP_cap_temp'] = -1
    
    if 'inv_temp2' in combined_df.columns:
        export_df['SP_relay_temp'] = combined_df['inv_temp2']
    else:
        export_df['SP_relay_temp'] = -1
    
    if 'inv_rh' in combined_df.columns:
        export_df['SP_internal_rh'] = combined_df['inv_rh']
    else:
        export_df['SP_internal_rh'] = -1
    
    # Identify columns that need cleaning (all SP columns except time and AE_GHI)
    sp_columns = ['SP_ambient_temp', 'SP_rh', 'SP_ac_power', 'SP_cap_temp', 'SP_relay_temp', 'SP_internal_rh']
    
    # Get AE_GHI values aligned with export_df
    ae_ghi_aligned = export_df['AE_GHI'].values
    
    # Clean SP data
    print("Cleaning SP data based on AE GHI values...")
    export_df_cleaned = clean_sp_data(
        export_df,
        ae_ghi_aligned,
        sp_columns
    )
    export_df = export_df_cleaned
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate output filename
    start_str = start_date.strftime('%Y%m%d')
    end_str = end_date.strftime('%Y%m%d')
    output_filename = f'export_data_{start_str}-{end_str}_{selected_device}.csv'
    output_path = os.path.join(output_dir, output_filename)
    
    # Export to CSV with only the requested columns in the specified order
    print(f"Exporting to {output_path}...")
    export_df = export_df[['time', 'AE_GHI', 'SP_ambient_temp', 'SP_rh', 'SP_ac_power', 'SP_cap_temp', 'SP_relay_temp', 'SP_internal_rh']]
    export_df.to_csv(output_path, index=False)
    
    print(f"Export completed! File saved to: {output_path}")
    print(f"Total rows: {len(export_df)}")
    print(f"Date range: {export_df['time'].min()} to {export_df['time'].max()}")
    
    return output_path


def main():
    """Main entry point."""
    args = parse_arguments()
    
    try:
        export_data(
            args.start_date,
            args.end_date,
            args.output_dir,
            device_id=args.device_id
        )
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
