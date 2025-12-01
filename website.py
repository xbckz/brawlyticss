"""
Local Web Server for Brawlnalytics
Full-featured with all bot pages and customizable themes
"""

from flask import Flask, render_template, request, redirect, session, jsonify
import json
import os
from datetime import datetime
import pandas as pd
from collections import defaultdict
import socket
from urllib.parse import unquote

from PIL import Image, ImageDraw, ImageFont
from flask import send_file
import io

app = Flask(__name__)
app.secret_key = os.urandom(24)

# File paths
TOKENS_FILE = 'data/tokens.json'
AUTHORIZED_USERS_FILE = 'data/authorized_users.json'
USER_SETTINGS_FILE = 'data/user_settings.json'
MATCHES_FILE = 'matches.xlsx'
TEAMS_FILE = 'teams.xlsx'

CONFIG = {
    'REGIONS': ['NA', 'EU', 'LATAM', 'EA', 'SEA'],
    'MODES': ['Gem Grab', 'Brawl Ball', 'Heist', 'Bounty', 'Knockout', 'Hot Zone']
}

# Theme presets
THEMES = {
    'red': {'primary': '#ef4444', 'bg': '#0a0a0a', 'card': '#111111', 'dark': '#1a1a1a'},
    'brawl': {'primary': '#e94560', 'bg': '#1a1a2e', 'card': '#16213e', 'dark': '#0f3460'},
    'purple': {'primary': '#8b5cf6', 'bg': '#1e1b29', 'card': '#2d2438', 'dark': '#1a1625'},
    'blue': {'primary': '#3b82f6', 'bg': '#0f172a', 'card': '#1e293b', 'dark': '#0f172a'},
    'green': {'primary': '#10b981', 'bg': '#064e3b', 'card': '#065f46', 'dark': '#022c22'},
    'orange': {'primary': '#f97316', 'bg': '#1c1917', 'card': '#292524', 'dark': '#1c1917'},
}

from functools import lru_cache
from datetime import datetime
import time

# Global cache variables
_cache = {
    'data': None,
    'timestamp': None,
    'user_settings_hash': None
}

CACHE_DURATION = 300  # Cache for 5 minutes (300 seconds)

def get_cache_key():
    """Generate a cache key based on user settings"""
    user_settings = load_json(USER_SETTINGS_FILE)
    user_id = str(session.get('discord_id', 'test_user'))
    user_prefs = user_settings.get(user_id, {})
    
    # Create a hash of relevant settings that affect data
    cache_key = f"{user_prefs.get('date_range', '30d')}_{user_prefs.get('start_date', '')}_{user_prefs.get('end_date', '')}"
    return cache_key

def get_cached_data():
    """Get cached data if valid, otherwise reload"""
    current_cache_key = get_cache_key()
    current_time = time.time()
    
    # Check if cache is valid
    if (_cache['data'] is not None and 
        _cache['timestamp'] is not None and
        _cache['user_settings_hash'] == current_cache_key and
        current_time - _cache['timestamp'] < CACHE_DURATION):
        
        print(f"✓ Using cached data (age: {int(current_time - _cache['timestamp'])}s)")
        return _cache['data']
    
    # Cache is invalid, reload data
    print("⟳ Loading fresh data...")
    start_time = time.time()
    
    data = load_matches_data()
    
    # Update cache
    _cache['data'] = data
    _cache['timestamp'] = current_time
    _cache['user_settings_hash'] = current_cache_key
    
    elapsed = time.time() - start_time
    print(f"✓ Data loaded in {elapsed:.2f}s")
    
    return data

def clear_cache():
    """Clear the cache (call this when settings change)"""
    _cache['data'] = None
    _cache['timestamp'] = None
    _cache['user_settings_hash'] = None
    print("✓ Cache cleared")

def load_json(filepath):
    if not os.path.exists(filepath):
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w') as f:
            json.dump({}, f)
        return {}
    with open(filepath, 'r') as f:
        return json.load(f)

def save_json(filepath, data):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)

def get_user_theme():
    """Get current user's theme or default"""
    if 'discord_id' not in session:
        return THEMES['red']
    
    settings = load_json(USER_SETTINGS_FILE)
    user_id = str(session['discord_id'])
    
    if user_id in settings:
        theme_name = settings[user_id].get('theme', 'red')
        return THEMES.get(theme_name, THEMES['red'])
    
    return THEMES['red']

def validate_token(token):
    tokens = load_json(TOKENS_FILE)
    if token not in tokens:
        return None, "Invalid token"
    token_data = tokens[token]
    if token_data.get('used', False):
        return None, "Token already used"
    return token_data, None

def mark_token_used(token):
    tokens = load_json(TOKENS_FILE)
    if token in tokens:
        tokens[token]['used'] = True
        save_json(TOKENS_FILE, tokens)

def is_user_authorized(discord_id):
    """Check if user is authorized and not expired"""
    authorized = load_json(AUTHORIZED_USERS_FILE)
    user_id = str(discord_id)
    
    if user_id not in authorized:
        return False
    
    user_data = authorized[user_id]
    
    # Check expiration
    expires_at = user_data.get('expires_at')
    if expires_at:
        expiration_date = pd.to_datetime(expires_at)
        if pd.Timestamp.now() > expiration_date:
            # Expired - remove from authorized list
            del authorized[user_id]
            save_json(AUTHORIZED_USERS_FILE, authorized)
            return False
    
    return True

def load_team_rosters():
    """Load valid player tags from teams.xlsx"""
    valid_players = {}
    
    if not os.path.exists(TEAMS_FILE):
        print(f"Warning: {TEAMS_FILE} not found - all players will be included")
        return None
    
    try:
        teams_df = pd.read_excel(TEAMS_FILE)
        
        for _, row in teams_df.iterrows():
            team_name = row['Team Name']
            if team_name not in valid_players:
                valid_players[team_name] = set()
            
            for i in range(1, 4):
                tag_col = f'Player {i} ID'
                if tag_col in teams_df.columns and pd.notna(row.get(tag_col)):
                    # Normalize: uppercase, strip, replace 0 with O
                    tag = str(row[tag_col]).strip().upper().replace('0', 'O')
                    valid_players[team_name].add(tag)
        
        print(f"Loaded rosters for {len(valid_players)} teams")
        return valid_players
    except Exception as e:
        print(f"Error loading team rosters: {e}")
        return None

def assign_brawlers_to_tiers_web(meta_scores):
    """
    Improved tier assignment for web server
    Creates balanced distributions using percentile-based approach
    """
    
    if not meta_scores:
        return None
    
    total_brawlers = len(meta_scores)
    
    # Define target percentages for each tier (more balanced)
    tier_percentages = {
        'S': 0.10,  # Top 10%
        'A': 0.20,  # Next 20%
        'B': 0.30,  # Next 30%
        'C': 0.25,  # Next 25%
        'D': 0.10,  # Next 10%
        'F': 0.05   # Bottom 5%
    }
    
    # Calculate target counts for each tier
    tier_targets = {}
    remaining = total_brawlers
    
    for tier in ['S', 'A', 'B', 'C', 'D']:
        count = max(1, int(total_brawlers * tier_percentages[tier]))
        tier_targets[tier] = count
        remaining -= count
    
    # F tier gets whatever is left (at least 0)
    tier_targets['F'] = max(0, remaining)
    
    # Assign brawlers to tiers based on counts
    tier_lists = {
        'S': [], 
        'A': [], 
        'B': [], 
        'C': [], 
        'D': [], 
        'F': []
    }
    
    tier_config = {
        'S': {'threshold': 0, 'color': '#ff4757', 'bg': '#3d1319'},
        'A': {'threshold': 0, 'color': '#ffa502', 'bg': '#3d2b0a'},
        'B': {'threshold': 0, 'color': '#ffd32a', 'bg': '#3d3610'},
        'C': {'threshold': 0, 'color': '#05c46b', 'bg': '#02311b'},
        'D': {'threshold': 0, 'color': '#0fbcf9', 'bg': '#043240'},
        'F': {'threshold': 0, 'color': '#747d8c', 'bg': '#1e2124'}
    }
    
    current_index = 0
    for tier_name in ['S', 'A', 'B', 'C', 'D', 'F']:
        count = tier_targets[tier_name]
        end_index = current_index + count
        
        # Assign brawlers
        tier_lists[tier_name] = meta_scores[current_index:end_index]
        
        # Set threshold based on lowest score in this tier
        if tier_lists[tier_name]:
            tier_config[tier_name]['threshold'] = tier_lists[tier_name][-1]['score']
        else:
            tier_config[tier_name]['threshold'] = 0
        
        current_index = end_index
    
    return tier_lists, tier_config


def load_matches_data():
    """Load and process all match data"""
    if not os.path.exists(MATCHES_FILE):
        return None, {}, {}, {}, set()
    
    try:
        df = pd.read_excel(MATCHES_FILE)
        
        # Apply date filtering based on user settings
        if 'battle_time' in df.columns:
            df['battle_time'] = pd.to_datetime(df['battle_time'], utc=True)
            
            # Get user's date filter preference
            user_settings = load_json(USER_SETTINGS_FILE)
            user_id = str(session.get('discord_id', 'test_user'))
            user_prefs = user_settings.get(user_id, {})
            date_range = user_prefs.get('date_range', '30d')
            
            if date_range == 'all':
                # No filtering - use all data
                pass
            elif date_range == '30d':
                # Last 30 days
                cutoff_date = pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=30)
                df = df[df['battle_time'] >= cutoff_date]
            elif date_range == 'custom':
                # Custom date range
                start_date = user_prefs.get('start_date')
                end_date = user_prefs.get('end_date')
                
                if start_date:
                    start_dt = pd.Timestamp(start_date, tz='UTC')
                    df = df[df['battle_time'] >= start_dt]
                
                if end_date:
                    # Add one day to include the entire end date
                    end_dt = pd.Timestamp(end_date, tz='UTC') + pd.Timedelta(days=1)
                    df = df[df['battle_time'] < end_dt]
        
        
        
        # Load valid rosters
        valid_rosters = load_team_rosters()
        
        teams_data = {}
        region_stats = defaultdict(lambda: {'total_matches': 0, 'teams': set()})
        mode_stats = defaultdict(lambda: defaultdict(int))
        all_brawlers = set()
        
        # ADD SERIES TRACKING - THIS IS THE KEY FIX
        series_tracking_brawlers = {}  # Track brawler picks per series
        
        for _, match in df.iterrows():
            match_id = match.get('battle_time', str(_))
            
            # CREATE SERIES ID (same logic as Discord bot)
            team1 = match['team1_name']
            team2 = match['team2_name']
            teams_sorted = tuple(sorted([team1, team2]))
            mode = str(match['mode'])
            map_name = str(match['map'])
            
            # Get both team compositions (sorted brawler lists)
            team1_comp = sorted([
                str(match['team1_player1_brawler']),
                str(match['team1_player2_brawler']),
                str(match['team1_player3_brawler'])
            ])
            team2_comp = sorted([
                str(match['team2_player1_brawler']),
                str(match['team2_player2_brawler']),
                str(match['team2_player3_brawler'])
            ])
            
            comps_sorted = tuple(sorted([tuple(team1_comp), tuple(team2_comp)]))
            
            # Round time to nearest 30 minutes
            battle_time = match.get('battle_time')
            if pd.notna(battle_time):
                time_rounded = pd.Timestamp(battle_time).floor('30min')
            else:
                time_rounded = match_id
            
            # Series ID: same teams + mode + map + comps + time window
            series_id = f"{teams_sorted}_{mode}_{map_name}_{comps_sorted}_{time_rounded}"
            
            if series_id not in series_tracking_brawlers:
                series_tracking_brawlers[series_id] = {}
            
            for team_prefix in ['team1', 'team2']:
                team_name = match[f'{team_prefix}_name']
                team_region = str(match[f'{team_prefix}_region']).strip().upper()
                
                if team_region in ['NAN', 'NONE', '', 'UNKNOWN'] or pd.isna(match[f'{team_prefix}_region']):
                    team_region = 'NA'
                
                if team_name not in teams_data:
                    teams_data[team_name] = {
                        'region': team_region,
                        'matches': 0,
                        'wins': 0,
                        'losses': 0,
                        'modes': defaultdict(lambda: {
                            'matches': 0,
                            'wins': 0,
                            'maps': defaultdict(lambda: {
                                'matches': 0,
                                'wins': 0,
                                'brawlers': defaultdict(lambda: {'picks': 0, 'wins': 0})
                            })
                        }),
                        'brawlers': defaultdict(lambda: {'picks': 0, 'wins': 0}),
                        'players': defaultdict(lambda: {
                            'matches': 0,
                            'wins': 0,
                            'star_player': 0,
                            'brawlers': defaultdict(lambda: {'picks': 0, 'wins': 0})
                        })
                    }
                
                team = teams_data[team_name]
                team['matches'] += 1
                
                winner_name = str(match['winner']).strip()
                is_winner = (winner_name == team_name)
                
                if is_winner:
                    team['wins'] += 1
                else:
                    team['losses'] += 1
                
                # Mode and map stats
                if mode not in ['Unknown', 'nan']:
                    team['modes'][mode]['matches'] += 1
                    team['modes'][mode]['maps'][map_name]['matches'] += 1
                    mode_stats[mode]['matches'] += 1
                    
                    if is_winner:
                        team['modes'][mode]['wins'] += 1
                        team['modes'][mode]['maps'][map_name]['wins'] += 1
                
                # Get star player tag once per team
                star_player_tag = str(match.get('star_player_tag', '')).strip().upper().replace('0', 'O')
                
                # Player and brawler stats
                for i in range(1, 4):
                    player_name = str(match.get(f'{team_prefix}_player{i}', ''))
                    player_tag = str(match.get(f'{team_prefix}_player{i}_tag', '')).strip().upper().replace('0', 'O')
                    brawler = str(match.get(f'{team_prefix}_player{i}_brawler', ''))
                    
                    if player_name and player_name != 'nan':
                        # Check if player is in the official roster
                        if valid_rosters and team_name in valid_rosters:
                            if player_tag not in valid_rosters[team_name]:
                                continue
                        
                        player = team['players'][player_tag]
                        player['name'] = player_name
                        player['matches'] += 1
                        
                        if is_winner:
                            player['wins'] += 1
                        
                        # Track star player
                        if star_player_tag and star_player_tag != 'NAN' and star_player_tag == player_tag:
                            player['star_player'] += 1
                        
                        if brawler and brawler != 'nan':
                            all_brawlers.add(brawler)
                            
                            # CRITICAL FIX: Track brawler picks per SERIES
                            if team_name not in series_tracking_brawlers[series_id]:
                                series_tracking_brawlers[series_id][team_name] = set()

                            brawler_key = f"{player_tag}_{brawler}"
                            if brawler_key not in series_tracking_brawlers[series_id][team_name]:
                                series_tracking_brawlers[series_id][team_name].add(brawler_key)
                                
                                # ONLY COUNT ONCE PER SERIES
                                player['brawlers'][brawler]['picks'] += 1
                                team['brawlers'][brawler]['picks'] += 1
                                
                                if mode not in ['Unknown', 'nan']:
                                    team['modes'][mode]['maps'][map_name]['brawlers'][brawler]['picks'] += 1
                                
                                # Only count win ONCE per series if they won
                                if is_winner:
                                    player['brawlers'][brawler]['wins'] += 1
                                    team['brawlers'][brawler]['wins'] += 1
                                    if mode not in ['Unknown', 'nan']:
                                        team['modes'][mode]['maps'][map_name]['brawlers'][brawler]['wins'] += 1
                
                region_stats[team_region]['total_matches'] += 1
                region_stats[team_region]['teams'].add(team_name)
        
        for region in region_stats:
            region_stats[region]['teams'] = list(region_stats[region]['teams'])
        
        return df, teams_data, dict(region_stats), dict(mode_stats), all_brawlers
    except Exception as e:
        print(f"Error loading matches: {e}")
        return None, {}, {}, {}, set()
    

@app.context_processor
def inject_theme():
    """Make theme available to all templates"""
    return {'theme': get_user_theme()}

@app.before_request
def require_auth():
    """Require authentication for all pages except auth"""
    if request.endpoint not in ['auth', 'index', 'static'] and 'discord_id' not in session:
        return redirect('/auth')

@app.route('/')
def index():
    if 'discord_id' in session:
        return redirect('/dashboard')
    return redirect('/auth')  # Force login in production

@app.route('/dashboard')
def dashboard():
    matches_df, teams_data, region_stats, mode_stats, all_brawlers = get_cached_data()
    if matches_df is None:
        return "Error loading data", 500
    
    top_teams = sorted(
        teams_data.items(),
        key=lambda x: (x[1]['wins'] / x[1]['matches']) if x[1]['matches'] > 0 else 0,
        reverse=True
    )[:10]
    
    return render_template('dashboard.html',
                         user=session['discord_tag'],
                         total_matches=len(matches_df),
                         total_teams=len(teams_data),
                         total_brawlers=len(all_brawlers),
                         top_teams=top_teams)

@app.route('/region/<region_name>')
def region_page(region_name):
    region_name = region_name.upper()
    matches_df, teams_data, region_stats, mode_stats, all_brawlers = get_cached_data()
    
    if matches_df is None:
        return "Error loading data", 500
    
    if region_name == 'ALL':
        region_teams = teams_data
        title = "All Regions"
    else:
        if region_name not in CONFIG['REGIONS']:
            return "Region not found", 404
        region_teams = {name: data for name, data in teams_data.items() if data['region'] == region_name}
        title = f"{region_name} Region"
    
    top_teams = sorted(
        region_teams.items(),
        key=lambda x: (x[1]['wins'] / x[1]['matches']) if x[1]['matches'] > 0 else 0,
        reverse=True
    )[:20]
    
    return render_template('region.html',
                         user=session['discord_tag'],
                         region=title,
                         region_code=region_name,
                         total_matches=sum(t['matches'] for t in region_teams.values()),
                         total_teams=len(region_teams),
                         top_teams=top_teams,
                         teams_data=teams_data)

@app.route('/team/<team_name>')
def team_page(team_name):
    _, teams_data, _, _, _ = get_cached_data()
    
    if team_name not in teams_data:
        return "Team not found", 404
    
    team = teams_data[team_name]
    
    return render_template('team.html',
                         user=session['discord_tag'],
                         team_name=team_name,
                         team=team)

@app.route('/team/<team_name>/mode/<mode>')
def team_mode_page(team_name, mode):
    _, teams_data, _, _, _ = get_cached_data()
    
    if team_name not in teams_data:
        return "Team not found", 404
    
    team = teams_data[team_name]
    
    if mode not in team['modes']:
        return "Mode not found", 404
    
    mode_data = team['modes'][mode]
    
    return render_template('team_mode.html',
                         user=session['discord_tag'],
                         team_name=team_name,
                         team=team,
                         mode=mode,
                         mode_data=mode_data)

@app.route('/team/<team_name>/mode/<mode>/map/<map_name>')
def team_map_page(team_name, mode, map_name):
    _, teams_data, _, _, _ = get_cached_data()
    
    if team_name not in teams_data:
        return "Team not found", 404
    
    team = teams_data[team_name]
    
    if mode not in team['modes'] or map_name not in team['modes'][mode]['maps']:
        return "Map not found", 404
    
    map_data = team['modes'][mode]['maps'][map_name]
    
    return render_template('team_map.html',
                         user=session['discord_tag'],
                         team_name=team_name,
                         team=team,
                         mode=mode,
                         map_name=map_name,
                         map_data=map_data)

@app.route('/settings', methods=['GET', 'POST'])
def settings():
    if request.method == 'POST':
        user_settings = load_json(USER_SETTINGS_FILE)
        user_id = str(session['discord_id'])
        
        theme = request.form.get('theme', 'red')
        date_range = request.form.get('date_range', '30d')
        start_date = request.form.get('start_date', '')
        end_date = request.form.get('end_date', '')
        
        # Save all settings
        user_settings[user_id] = {
            'theme': theme if theme in THEMES else 'red',
            'date_range': date_range,
            'start_date': start_date,
            'end_date': end_date
        }
        save_json(USER_SETTINGS_FILE, user_settings)
        
        # IMPORTANT: Clear cache when settings change
        clear_cache()
        
        session.modified = True
        
        return redirect('/settings')
    
    # GET request - load current settings
    current_settings = load_json(USER_SETTINGS_FILE)
    user_id = str(session['discord_id'])
    user_prefs = current_settings.get(user_id, {})
    
    current_theme = user_prefs.get('theme', 'red')
    current_date_range = user_prefs.get('date_range', '30d')
    start_date = user_prefs.get('start_date', '')
    end_date = user_prefs.get('end_date', '')
    
    return render_template('settings.html',
                         user=session['discord_tag'],
                         themes=THEMES,
                         current_theme=current_theme,
                         current_date_range=current_date_range,
                         start_date=start_date,
                         end_date=end_date)

@app.route('/logout')
def logout():
    session.clear()
    return redirect('/')

@app.route('/player/<path:player_tag>')
def player_page(player_tag):
    """Display individual player statistics"""
    # Decode the URL-encoded tag
    player_tag = unquote(player_tag)
    
    _, teams_data, _, _, _ = get_cached_data()
    
    # Search through all teams to find the player
    player_data = None
    team_name = None
    
    for t_name, team in teams_data.items():
        if player_tag in team['players']:
            player_data = team['players'][player_tag]
            team_name = t_name
            break
    
    if not player_data:
        return f"Player not found: {player_tag}", 404
    
    # Find favorite (most played) brawler
    favorite_brawler = None
    if player_data['brawlers']:
        favorite_brawler_name = max(
            player_data['brawlers'].items(),
            key=lambda x: x[1]['picks']
        )
        favorite_brawler = {
            'name': favorite_brawler_name[0],
            'picks': favorite_brawler_name[1]['picks'],
            'wins': favorite_brawler_name[1]['wins']
        }
    else:
        # Default if no brawlers
        favorite_brawler = {
            'name': 'None',
            'picks': 0,
            'wins': 0
        }
    
    # Build player object for template
    player = {
        'name': player_data['name'],
        'tag': player_tag,
        'team_name': team_name,
        'region': teams_data[team_name]['region'],
        'matches': player_data['matches'],
        'wins': player_data['wins'],
        'star_player': player_data['star_player'],
        'favorite_brawler': favorite_brawler,
        'brawlers': player_data['brawlers']
    }
    
    return render_template('player.html',
                         user=session['discord_tag'],
                         player=player)



@app.route('/meta')
def meta_page():
    # Get filter parameters
    region = request.args.get('region', 'ALL').upper()
    mode = request.args.get('mode', 'ALL')
    
    _, teams_data, _, _, _ = get_cached_data()
    
    # Collect brawler stats based on filters
    # Use the SAME data source as load_matches_data already processed
    brawler_stats = defaultdict(lambda: {
        'picks': 0,
        'wins': 0
    })
    
    total_picks = 0
    
    for team_name, team in teams_data.items():
        team_region = team['region']
        
        # Filter by region
        if region != 'ALL' and team_region != region:
            continue
        
        for mode_name, mode_data in team['modes'].items():
            if mode_name in ['Unknown', 'nan', '', 'None']:
                continue
            
            # Filter by mode
            if mode != 'ALL' and mode_name != mode:
                continue
                
            for map_name, map_data in mode_data['maps'].items():
                for brawler, brawler_data in map_data['brawlers'].items():
                    # This data is ALREADY deduplicated by load_matches_data
                    brawler_stats[brawler]['picks'] += brawler_data['picks']
                    brawler_stats[brawler]['wins'] += brawler_data['wins']
                    total_picks += brawler_data['picks']
    
    
    meta_brawlers = []
    for brawler, data in brawler_stats.items():
        if data['picks'] >= 1:
            pick_rate = (data['picks'] / total_picks) * 100 if total_picks > 0 else 0
            win_rate = (data['wins'] / data['picks']) * 100 if data['picks'] > 0 else 0
            meta_score = win_rate * pick_rate
            meta_brawlers.append((brawler, data, meta_score))
    
    meta_brawlers.sort(key=lambda x: x[2], reverse=True)
    
    # Get just brawler and data (without score) for template
    meta_brawlers = [(b, d) for b, d, _ in meta_brawlers]
    
    # Get all modes for filter buttons
    all_modes = set()
    for team in teams_data.values():
        for mode_name in team['modes'].keys():
            if mode_name not in ['Unknown', 'nan', '', 'None']:
                all_modes.add(mode_name)
    
    return render_template('meta.html',
                         user=session['discord_tag'],
                         meta_brawlers=meta_brawlers,
                         total_picks=total_picks,
                         modes=sorted(all_modes),
                         current_region=region,
                         current_mode=mode)

@app.route('/api/meta/generate')
def generate_meta_tier_list():
    """Generate tier list image based on filters"""
    try:
        region = request.args.get('region', 'ALL').upper()
        mode = request.args.get('mode', 'ALL')
        
        print(f"Generating tier list for region={region}, mode={mode}")
        
        _, teams_data, _, _, _ = get_cached_data()
        
        if not teams_data:
            print("No teams data available")
            return "No data available", 404
    except:
        print("!")
        
    # Collect brawler stats based on filters
    brawler_stats = defaultdict(lambda: {'picks': 0, 'wins': 0})
    total_picks = 0
    
    for team_name, team in teams_data.items():
        # Filter by region
        if region != 'ALL' and team['region'] != region:
            continue
        
        # Iterate through modes
        for mode_name, mode_data in team['modes'].items():
            if mode_name in ['Unknown', 'nan', '', 'None']:
                continue
            
            # Filter by mode
            if mode != 'ALL' and mode_name != mode:
                continue
            
            for map_name, map_data in mode_data['maps'].items():
                for brawler, brawler_data in map_data['brawlers'].items():
                    brawler_stats[brawler]['picks'] += brawler_data['picks']
                    brawler_stats[brawler]['wins'] += brawler_data['wins']
                    total_picks += brawler_data['picks']
    
    # Calculate meta scores
    meta_scores = []
    for brawler, data in brawler_stats.items():
        if data['picks'] < 1:  # Skip low sample size
            continue
        
        pick_rate = (data['picks'] / total_picks * 100) if total_picks > 0 else 0
        win_rate = (data['wins'] / data['picks'] * 100) if data['picks'] > 0 else 0
        meta_score = (win_rate * pick_rate) / 100
        
        meta_scores.append({
            'name': brawler,
            'score': meta_score,
            'pick_rate': pick_rate,
            'win_rate': win_rate,
            'picks': data['picks']
        })
    
    if not meta_scores:
        return "Not enough data", 404
    
    # Sort by meta score
    meta_scores.sort(key=lambda x: x['score'], reverse=True)
    
    # Use improved tier assignment
    tier_lists, tier_config = assign_brawlers_to_tiers_web(meta_scores)
    
    if not tier_lists:
        return "Not enough data", 404

    print("\n" + "="*80)
    print("WEB SERVER - TIER ASSIGNMENTS")
    print("="*80)
    print(f"Total brawlers: {len(meta_scores)}")
    
    print("\nTier contents:")
    for tier_name in ['S', 'A', 'B', 'C', 'D', 'F']:
        brawlers = tier_lists[tier_name]
        print(f"\n{tier_name} Tier ({len(brawlers)} brawlers):")
        for b in brawlers:
            print(f"  {b['name']}: score={b['score']:.4f}, wr={b['win_rate']:.2f}%, pr={b['pick_rate']:.2f}%")
    print("="*80 + "\n")
    
    # Generate image
    print(f"Generating image with {sum(len(t) for t in tier_lists.values())} brawlers")
    img = generate_tier_list_image(tier_lists, region, mode, tier_config)
    
    # Send image
    img_io = io.BytesIO()
    img.save(img_io, 'PNG', optimize=False)
    img_io.seek(0)
    
    print("Image generated successfully")
    return send_file(img_io, mimetype='image/png')


def generate_tier_list_image(tier_lists, region, mode, tier_config):
    """Create the actual tier list image"""
    
    # Image dimensions - Closer spacing
    card_size = 60
    spacing = 8
    tier_box_width = 60
    max_brawlers_per_row = 14
    name_height = 16  # Height for brawler name
    
    # FIXED width based on max brawlers per row
    img_width = tier_box_width + (max_brawlers_per_row * (card_size + spacing)) + 8
    tier_height = card_size + name_height + 12  # Include space for name
    header_height = 160
    padding = 15
    
    # Calculate total rows needed
    active_tiers = [t for t in ['S', 'A', 'B', 'C', 'D', 'F'] if tier_lists[t]]
    total_rows = 0
    for tier in active_tiers:
        brawlers_in_tier = len(tier_lists[tier])
        rows_for_tier = (brawlers_in_tier + max_brawlers_per_row - 1) // max_brawlers_per_row
        total_rows += rows_for_tier
    
    img_height = header_height + (tier_height * total_rows) 
    
    # Create image
    img = Image.new('RGB', (img_width, img_height), color='#0a0a0a')
    draw = ImageDraw.Draw(img)
    
    # Load fonts
    try:
        font_paths = [
            "arial.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/System/Library/Fonts/Helvetica.ttc",
            "C:\\Windows\\Fonts\\arial.ttf"
        ]
        
        title_font = None
        for font_path in font_paths:
            try:
                title_font = ImageFont.truetype(font_path, 55)
                subtitle_font = ImageFont.truetype(font_path, 30)
                tier_font = ImageFont.truetype(font_path, 27)
                name_font = ImageFont.truetype(font_path, 10)  # Small font for names
                break
            except:
                continue
        
        if title_font is None:
            raise Exception("No font found")
    except:
        print("Using default font")
        title_font = ImageFont.load_default()
        subtitle_font = ImageFont.load_default()
        tier_font = ImageFont.load_default()
        name_font = ImageFont.load_default()
    
    # Draw header
    region_text = "All Regions" if region == 'ALL' else f"{region} Region"
    mode_text = f" - {mode}" if mode != 'ALL' else ""
    title = f"Meta Tier List"
    subtitle = f"{region_text}{mode_text}"
    
    # Draw header background
    draw.rectangle([0, 0, img_width, header_height], fill='#1e1e2e')
    
    # Title
    bbox = draw.textbbox((0, 0), title, font=title_font)
    text_width = bbox[2] - bbox[0]
    draw.text(((img_width - text_width) // 2, 25), title, font=title_font, fill='#ffffff')
    
    # Subtitle
    bbox = draw.textbbox((0, 0), subtitle, font=subtitle_font)
    text_width = bbox[2] - bbox[0]
    draw.text(((img_width - text_width) // 2, 115), subtitle, font=subtitle_font, fill='#c0c0c0')
    
    # Draw each tier
    y_offset = header_height
    
    for tier_index, tier in enumerate(active_tiers):
        brawlers = tier_lists[tier]
        
        # Get tier config colors
        tier_data = tier_config[tier]
        color = tier_data['color']
        bg_color = tier_data['bg']
        
        # Split brawlers into rows
        brawler_rows = []
        for i in range(0, len(brawlers), max_brawlers_per_row):
            brawler_rows.append(brawlers[i:i + max_brawlers_per_row])
        
        tier_total_height = tier_height * len(brawler_rows)
        
        # Draw tier background
        draw.rectangle(
            [(0, y_offset), (img_width, y_offset + tier_total_height)],
            fill='#282838'
        )
        
        # Draw tier label
        draw.rectangle(
            [(0, y_offset), (tier_box_width, y_offset + tier_total_height)],
            fill=color
        )
        
        bbox = draw.textbbox((0, 0), tier, font=tier_font)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]
        draw.text(
            ((tier_box_width - text_width) // 2, y_offset + (tier_total_height - text_height) // 2),
            tier,
            fill=(0, 0, 0),
            font=tier_font
        )
        
        # Draw brawlers row by row
        current_row_y = y_offset
        for brawler_row in brawler_rows:
            x_offset = tier_box_width + spacing
            
            for brawler_data in brawler_row:
                brawler_name = brawler_data['name']
                
                # Try to load brawler image
                brawler_img_path = f"static/images/brawlers/{brawler_name.lower().replace(' ', '_').replace('-', '_')}.png"
                
                try:
                    if os.path.exists(brawler_img_path):
                        brawler_img = Image.open(brawler_img_path).convert('RGBA')
                        brawler_img.thumbnail((card_size - 10, card_size - 10), Image.Resampling.LANCZOS)
                        # Paste centered
                        paste_x = x_offset + (card_size - brawler_img.width) // 2
                        paste_y = current_row_y + 6 + (card_size - brawler_img.height) // 2
                        img.paste(brawler_img, (paste_x, paste_y), brawler_img)
                except Exception as e:
                    print(f"Error loading brawler image {brawler_name}: {e}")
                    # Draw placeholder
                    draw.rectangle(
                        [(x_offset + 5, current_row_y + 11), 
                         (x_offset + card_size - 5, current_row_y + 6 + card_size - 5)],
                        fill=(80, 80, 80)
                    )
                
                # Draw brawler name under image
                display_name = brawler_name if len(brawler_name) <= 9 else brawler_name[:7] + ".."
                name_bbox = draw.textbbox((0, 0), display_name, font=name_font)
                name_width = name_bbox[2] - name_bbox[0]
                draw.text(
                    (x_offset + (card_size - name_width) // 2, current_row_y + card_size + 4),
                    display_name,
                    fill=(200, 200, 200),
                    font=name_font
                )
                
                x_offset += card_size + spacing
            
            current_row_y += tier_height
        
        y_offset = current_row_y
        
        # **NEW: Draw separator line between tiers (except after last tier)**
        if tier_index < len(active_tiers) - 1:
            # Draw a horizontal line at y_offset
            draw.line(
                [(0, y_offset), (img_width, y_offset)],
                fill='#0a0a0a',  # Match background color for a dividing line
                width=3
            )
    
    return img

@app.route('/brawlers')
def brawlers_page():
    """Main brawlers overview page"""
    _, teams_data, _, _, all_brawlers = get_cached_data()
    
    # Collect comprehensive brawler stats
    brawler_stats = defaultdict(lambda: {
        'picks': 0,
        'wins': 0,
        'modes': defaultdict(lambda: {'picks': 0, 'wins': 0}),
        'maps': defaultdict(lambda: {'picks': 0, 'wins': 0}),
        'teammates': defaultdict(lambda: {'picks': 0, 'wins': 0}),
        'opponents': defaultdict(lambda: {'picks': 0, 'wins': 0})
    })
    
    total_picks = 0
    
    for team_name, team in teams_data.items():
        for mode, mode_data in team['modes'].items():
            if mode in ['Unknown', 'nan', '', 'None']:
                continue
                
            for map_name, map_data in mode_data['maps'].items():
                for brawler, brawler_data in map_data['brawlers'].items():
                    stats = brawler_stats[brawler]
                    stats['picks'] += brawler_data['picks']
                    stats['wins'] += brawler_data['wins']
                    stats['modes'][mode]['picks'] += brawler_data['picks']
                    stats['modes'][mode]['wins'] += brawler_data['wins']
                    stats['maps'][map_name]['picks'] += brawler_data['picks']
                    stats['maps'][map_name]['wins'] += brawler_data['wins']
                    total_picks += brawler_data['picks']
    
    # Sort brawlers by picks
    brawlers_list = []
    for brawler, data in brawler_stats.items():
        if data['picks'] >= 2:
            win_rate = (data['wins'] / data['picks'] * 100) if data['picks'] > 0 else 0
            pick_rate = (data['picks'] / total_picks * 100) if total_picks > 0 else 0
            brawlers_list.append({
                'name': brawler,
                'picks': data['picks'],
                'wins': data['wins'],
                'win_rate': win_rate,
                'pick_rate': pick_rate
            })
    
    brawlers_list.sort(key=lambda x: x['picks'], reverse=True)
    
    return render_template('brawlers.html',
                         user=session['discord_tag'],
                         brawlers=brawlers_list,
                         total_picks=total_picks)


@app.route('/brawler/<brawler_name>')
def brawler_detail_page(brawler_name):
    """Detailed brawler statistics page"""
    matches_df, teams_data, _, _, _ = get_cached_data()
    
    if matches_df is None:
        return "Error loading data", 500
    
    # Collect detailed stats for this brawler
    brawler_stats = {
        'picks': 0,
        'wins': 0,
        'modes': defaultdict(lambda: {
            'picks': 0,
            'wins': 0,
            'maps': defaultdict(lambda: {'picks': 0, 'wins': 0})
        }),
        'teammates': defaultdict(lambda: {'picks': 0, 'wins': 0}),
        'opponents': defaultdict(lambda: {'picks': 0, 'wins': 0})
    }
    
    # Track series to avoid double-counting
    series_tracking = {}
    
    # Track matchups using match data
    for _, match in matches_df.iterrows():
        # Create series ID (same logic as load_matches_data)
        team1 = match['team1_name']
        team2 = match['team2_name']
        teams_sorted = tuple(sorted([team1, team2]))
        mode = str(match['mode'])
        map_name = str(match['map'])
        
        team1_comp = sorted([
            str(match['team1_player1_brawler']),
            str(match['team1_player2_brawler']),
            str(match['team1_player3_brawler'])
        ])
        team2_comp = sorted([
            str(match['team2_player1_brawler']),
            str(match['team2_player2_brawler']),
            str(match['team2_player3_brawler'])
        ])
        
        comps_sorted = tuple(sorted([tuple(team1_comp), tuple(team2_comp)]))
        
        battle_time = match.get('battle_time')
        if pd.notna(battle_time):
            time_rounded = pd.Timestamp(battle_time).floor('30min')
        else:
            time_rounded = str(_)
        
        series_id = f"{teams_sorted}_{mode}_{map_name}_{comps_sorted}_{time_rounded}"
        
        for team_prefix in ['team1', 'team2']:
            team_brawlers = [
                str(match[f'{team_prefix}_player1_brawler']),
                str(match[f'{team_prefix}_player2_brawler']),
                str(match[f'{team_prefix}_player3_brawler'])
            ]
            
            if brawler_name not in team_brawlers:
                continue
            
            # Check if we've already counted this series for this brawler
            if series_id not in series_tracking:
                series_tracking[series_id] = set()
            
            if brawler_name in series_tracking[series_id]:
                continue  # Already counted this series
            
            series_tracking[series_id].add(brawler_name)
            
            # Found our brawler in this team
            winner = str(match['winner'])
            team_name = match[f'{team_prefix}_name']
            is_winner = (winner == team_name)
            
            # Update basic stats (ONCE per series)
            brawler_stats['picks'] += 1
            if is_winner:
                brawler_stats['wins'] += 1
            
            # Mode and map stats
            if mode not in ['Unknown', 'nan', '', 'None']:
                brawler_stats['modes'][mode]['picks'] += 1
                brawler_stats['modes'][mode]['maps'][map_name]['picks'] += 1
                if is_winner:
                    brawler_stats['modes'][mode]['wins'] += 1
                    brawler_stats['modes'][mode]['maps'][map_name]['wins'] += 1
            
            # Teammates (other brawlers on same team)
            for teammate in team_brawlers:
                if teammate != brawler_name and teammate not in ['nan', '', 'None']:
                    brawler_stats['teammates'][teammate]['picks'] += 1
                    if is_winner:
                        brawler_stats['teammates'][teammate]['wins'] += 1
            
            # Opponents (brawlers on enemy team)
            enemy_prefix = 'team2' if team_prefix == 'team1' else 'team1'
            enemy_brawlers = [
                str(match[f'{enemy_prefix}_player1_brawler']),
                str(match[f'{enemy_prefix}_player2_brawler']),
                str(match[f'{enemy_prefix}_player3_brawler'])
            ]
            
            for opponent in enemy_brawlers:
                if opponent not in ['nan', '', 'None']:
                    brawler_stats['opponents'][opponent]['picks'] += 1
                    if is_winner:
                        brawler_stats['opponents'][opponent]['wins'] += 1
    
    if brawler_stats['picks'] == 0:
        return "Brawler not found or no data available", 404
    
    # Calculate win rates and sort
    overall_winrate = (brawler_stats['wins'] / brawler_stats['picks'] * 100) if brawler_stats['picks'] > 0 else 0
    
    # Best modes (sort by win rate with min 3 picks)
    best_modes = []
    for mode, data in brawler_stats['modes'].items():
        if data['picks'] >= 3:
            wr = (data['wins'] / data['picks'] * 100) if data['picks'] > 0 else 0
            best_modes.append({'name': mode, 'picks': data['picks'], 'wins': data['wins'], 'win_rate': wr})
    best_modes.sort(key=lambda x: x['win_rate'], reverse=True)
    
    # Best maps (sort by win rate with min 3 picks)
    best_maps = []
    for mode, mode_data in brawler_stats['modes'].items():
        for map_name, data in mode_data['maps'].items():
            if data['picks'] >= 3:
                wr = (data['wins'] / data['picks'] * 100) if data['picks'] > 0 else 0
                best_maps.append({
                    'name': map_name,
                    'mode': mode,
                    'picks': data['picks'],
                    'wins': data['wins'],
                    'win_rate': wr
                })
    best_maps.sort(key=lambda x: x['win_rate'], reverse=True)
    
    # Best teammates (sort by win rate with min 5 picks together)
    best_teammates = []
    for teammate, data in brawler_stats['teammates'].items():
        if data['picks'] >= 5:
            wr = (data['wins'] / data['picks'] * 100) if data['picks'] > 0 else 0
            best_teammates.append({'name': teammate, 'picks': data['picks'], 'wins': data['wins'], 'win_rate': wr})
    best_teammates.sort(key=lambda x: x['win_rate'], reverse=True)
    
    # FIXED: Calculate ALL matchups, then split into best/worst
    all_matchups = []
    for opponent, data in brawler_stats['opponents'].items():
        if data['picks'] >= 5:
            wr = (data['wins'] / data['picks'] * 100) if data['picks'] > 0 else 0
            all_matchups.append({
                'name': opponent,
                'picks': data['picks'],
                'wins': data['wins'],
                'win_rate': wr
            })
    
    # Sort by win rate
    all_matchups.sort(key=lambda x: x['win_rate'], reverse=True)
    
    # Best matchups: Top 10 highest win rates (we beat them)
    best_matchups = all_matchups[:10]
    
    # Worst matchups: Bottom 10 lowest win rates (they beat us)
    worst_matchups = all_matchups[-10:]
    worst_matchups.reverse()  # Show worst first (lowest win rate at top)
    
    return render_template('brawler_detail.html',
                         user=session['discord_tag'],
                         brawler_name=brawler_name,
                         stats=brawler_stats,
                         overall_winrate=overall_winrate,
                         best_modes=best_modes[:10],
                         best_maps=best_maps[:10],
                         best_teammates=best_teammates[:10],
                         best_matchups=best_matchups,
                         worst_matchups=worst_matchups)


@app.route('/modes/<mode_name>')
def mode_detail_page(mode_name):
    """Detailed mode statistics page"""
    # Convert URL format back to display format
    mode_display = mode_name.replace('_', ' ').title()
    
    matches_df, teams_data, _, _, _ = get_cached_data()
    
    if matches_df is None:
        return "Error loading data", 500
    
    # Collect stats for this mode
    mode_stats = {
        'total_games': 0,
        'brawlers': defaultdict(lambda: {'picks': 0, 'wins': 0}),
        'maps': defaultdict(lambda: {'games': 0})
    }
    
    for team_name, team in teams_data.items():
        for mode, mode_data in team['modes'].items():
            if mode.lower().replace(' ', '_') != mode_name.lower():
                continue
            
            mode_display = mode  # Use the actual mode name from data
            
            # Count games per map
            for map_name, map_data in mode_data['maps'].items():
                # Use 'matches' key instead of 'picks'
                games = map_data.get('matches', 0)
                mode_stats['total_games'] += games
                mode_stats['maps'][map_name]['games'] += games
                
                # Collect brawler stats
                for brawler, brawler_data in map_data.get('brawlers', {}).items():
                    mode_stats['brawlers'][brawler]['picks'] += brawler_data.get('picks', 0)
                    mode_stats['brawlers'][brawler]['wins'] += brawler_data.get('wins', 0)
    
    if mode_stats['total_games'] == 0:
        return "Mode not found or no data available", 404
    
    # Calculate best brawlers (min 5 picks)
    best_brawlers = []
    for brawler, data in mode_stats['brawlers'].items():
        if data['picks'] >= 1:
            win_rate = (data['wins'] / data['picks'] * 100) if data['picks'] > 0 else 0
            best_brawlers.append({
                'name': brawler,
                'picks': data['picks'],
                'wins': data['wins'],
                'win_rate': win_rate
            })
    
    best_brawlers.sort(key=lambda x: x['win_rate'], reverse=True)
    
    # Get maps list
    maps_list = []
    for map_name, data in mode_stats['maps'].items():
        maps_list.append({
            'name': map_name,
            'picks': data['games']
        })
    
    maps_list.sort(key=lambda x: x['picks'], reverse=True)
    
    # Calculate total picks for meta score
    total_picks = sum(b['picks'] for b in best_brawlers)

    return render_template('mode_detail.html',
                        user=session['discord_tag'],
                        mode_name=mode_display,
                        total_games=mode_stats['total_games'],
                        total_maps=len(mode_stats['maps']),
                        total_brawlers=len(mode_stats['brawlers']),
                        best_brawlers=best_brawlers,
                        maps=maps_list,
                        total_picks=total_picks)


@app.route('/maps/<map_name>')
def map_detail_page(map_name):
    """Detailed map statistics page"""
    # Convert URL format back to display format
    map_display = map_name.replace('_', ' ').title()
    
    matches_df, teams_data, _, _, _ = get_cached_data()
    
    if matches_df is None:
        return "Error loading data", 500
    
    # Collect stats for this map
    map_stats = {
        'total_games': 0,
        'mode': None,
        'brawlers': defaultdict(lambda: {'picks': 0, 'wins': 0})
    }
    
    for team_name, team in teams_data.items():
        for mode, mode_data in team['modes'].items():
            for map_n, map_data in mode_data.get('maps', {}).items():
                # Match the map name (case-insensitive, handle formatting)
                if map_n.lower().replace(' ', '_').replace("'", '').replace('-', '_') != map_name.lower():
                    continue
                
                map_display = map_n  # Use the actual map name from data
                map_stats['mode'] = mode
                # Use 'matches' key instead of 'picks'
                map_stats['total_games'] += map_data.get('matches', 0)
                
                # Collect brawler stats
                for brawler, brawler_data in map_data.get('brawlers', {}).items():
                    map_stats['brawlers'][brawler]['picks'] += brawler_data.get('picks', 0)
                    map_stats['brawlers'][brawler]['wins'] += brawler_data.get('wins', 0)
    
    if map_stats['total_games'] == 0:
        return "Map not found or no data available", 404
    
    # Calculate best brawlers (min 3 picks for maps since they have less data)
    best_brawlers = []
    for brawler, data in map_stats['brawlers'].items():
        if data['picks'] >= 1:
            win_rate = (data['wins'] / data['picks'] * 100) if data['picks'] > 0 else 0
            best_brawlers.append({
                'name': brawler,
                'picks': data['picks'],
                'wins': data['wins'],
                'win_rate': win_rate
            })
    
    # Calculate total picks for meta score
    total_picks = sum(b['picks'] for b in best_brawlers)

    return render_template('map_detail.html',
                        user=session['discord_tag'],
                        map_name=map_display,
                        mode_name=map_stats['mode'],
                        total_games=map_stats['total_games'],
                        total_brawlers=len(map_stats['brawlers']),
                        best_brawlers=best_brawlers,
                        total_picks=total_picks)

# Add these routes to your Flask app (around line 950, after the brawler_detail_page route)
# Add these routes to your Flask app (around line 950, after the brawler_detail_page route)

@app.route('/brawler/<brawler_name>/mode/<mode_name>')
def brawler_mode_page(brawler_name, mode_name):
    """Brawler performance in a specific mode"""
    matches_df, teams_data, _, _, _ = get_cached_data()
    
    if matches_df is None:
        return "Error loading data", 500
    
    # Convert URL format to display format
    mode_display = mode_name.replace('_', ' ').title()
    
    # Collect stats for this brawler in this mode
    brawler_mode_stats = {
        'picks': 0,
        'wins': 0,
        'maps': []
    }
    
    # Track series to avoid double-counting
    series_tracking = {}
    
    for _, match in matches_df.iterrows():
        mode = str(match['mode'])
        
        # Skip if not the right mode
        if mode.lower().replace(' ', '_') != mode_name.lower():
            continue
        
        mode_display = mode  # Use actual mode name from data
        
        # Create series ID
        team1 = match['team1_name']
        team2 = match['team2_name']
        teams_sorted = tuple(sorted([team1, team2]))
        map_name = str(match['map'])
        
        team1_comp = sorted([
            str(match['team1_player1_brawler']),
            str(match['team1_player2_brawler']),
            str(match['team1_player3_brawler'])
        ])
        team2_comp = sorted([
            str(match['team2_player1_brawler']),
            str(match['team2_player2_brawler']),
            str(match['team2_player3_brawler'])
        ])
        
        comps_sorted = tuple(sorted([tuple(team1_comp), tuple(team2_comp)]))
        
        battle_time = match.get('battle_time')
        if pd.notna(battle_time):
            time_rounded = pd.Timestamp(battle_time).floor('30min')
        else:
            time_rounded = str(_)
        
        series_id = f"{teams_sorted}_{mode}_{map_name}_{comps_sorted}_{time_rounded}"
        
        for team_prefix in ['team1', 'team2']:
            team_brawlers = [
                str(match[f'{team_prefix}_player1_brawler']),
                str(match[f'{team_prefix}_player2_brawler']),
                str(match[f'{team_prefix}_player3_brawler'])
            ]
            
            if brawler_name not in team_brawlers:
                continue
            
            # Check if already counted
            if series_id not in series_tracking:
                series_tracking[series_id] = {}
            
            if map_name not in series_tracking[series_id]:
                series_tracking[series_id][map_name] = set()
            
            if brawler_name in series_tracking[series_id][map_name]:
                continue
            
            series_tracking[series_id][map_name].add(brawler_name)
            
            # Update stats
            winner = str(match['winner'])
            team_name = match[f'{team_prefix}_name']
            is_winner = (winner == team_name)
            
            brawler_mode_stats['picks'] += 1
            if is_winner:
                brawler_mode_stats['wins'] += 1
    
    if brawler_mode_stats['picks'] == 0:
        return "No data available for this combination", 404
    
    # Collect map stats from teams_data
    map_stats = defaultdict(lambda: {'picks': 0, 'wins': 0})
    
    for team_name, team in teams_data.items():
        for mode, mode_data in team['modes'].items():
            if mode.lower().replace(' ', '_') != mode_name.lower():
                continue
            
            for map_name, map_data in mode_data['maps'].items():
                if brawler_name in map_data.get('brawlers', {}):
                    brawler_data = map_data['brawlers'][brawler_name]
                    map_stats[map_name]['picks'] += brawler_data['picks']
                    map_stats[map_name]['wins'] += brawler_data['wins']
    
    # Convert to list and calculate win rates
    maps = []
    for map_name, data in map_stats.items():
        if data['picks'] >= 3:  # Minimum 3 picks
            win_rate = (data['wins'] / data['picks'] * 100) if data['picks'] > 0 else 0
            maps.append({
                'name': map_name,
                'picks': data['picks'],
                'wins': data['wins'],
                'win_rate': win_rate
            })
    
    maps.sort(key=lambda x: x['win_rate'], reverse=True)
    
    # Track teammates and opponents for this brawler in this mode
    teammates_stats = defaultdict(lambda: {'picks': 0, 'wins': 0})
    opponent_stats = defaultdict(lambda: {'picks': 0, 'wins': 0})
    
    # Reset series tracking for teammates/opponents
    series_tracking_synergy = {}
    
    for _, match in matches_df.iterrows():
        mode = str(match['mode'])
        
        # Skip if not the right mode
        if mode.lower().replace(' ', '_') != mode_name.lower():
            continue
        
        # Create series ID
        team1 = match['team1_name']
        team2 = match['team2_name']
        teams_sorted = tuple(sorted([team1, team2]))
        map_name = str(match['map'])
        
        team1_comp = sorted([
            str(match['team1_player1_brawler']),
            str(match['team1_player2_brawler']),
            str(match['team1_player3_brawler'])
        ])
        team2_comp = sorted([
            str(match['team2_player1_brawler']),
            str(match['team2_player2_brawler']),
            str(match['team2_player3_brawler'])
        ])
        
        comps_sorted = tuple(sorted([tuple(team1_comp), tuple(team2_comp)]))
        
        battle_time = match.get('battle_time')
        if pd.notna(battle_time):
            time_rounded = pd.Timestamp(battle_time).floor('30min')
        else:
            time_rounded = str(_)
        
        series_id = f"{teams_sorted}_{mode}_{map_name}_{comps_sorted}_{time_rounded}"
        
        if series_id not in series_tracking_synergy:
            series_tracking_synergy[series_id] = set()
        
        for team_prefix in ['team1', 'team2']:
            team_brawlers = [
                str(match[f'{team_prefix}_player1_brawler']),
                str(match[f'{team_prefix}_player2_brawler']),
                str(match[f'{team_prefix}_player3_brawler'])
            ]
            
            if brawler_name not in team_brawlers:
                continue
            
            # Check if already counted
            if brawler_name in series_tracking_synergy[series_id]:
                continue
            
            series_tracking_synergy[series_id].add(brawler_name)
            
            # Update stats
            winner = str(match['winner'])
            team_name = match[f'{team_prefix}_name']
            is_winner = (winner == team_name)
            
            # Track teammates
            for teammate in team_brawlers:
                if teammate != brawler_name and teammate not in ['nan', '', 'None']:
                    teammates_stats[teammate]['picks'] += 1
                    if is_winner:
                        teammates_stats[teammate]['wins'] += 1
            
            # Track opponents
            enemy_prefix = 'team2' if team_prefix == 'team1' else 'team1'
            enemy_brawlers = [
                str(match[f'{enemy_prefix}_player1_brawler']),
                str(match[f'{enemy_prefix}_player2_brawler']),
                str(match[f'{enemy_prefix}_player3_brawler'])
            ]
            
            for opponent in enemy_brawlers:
                if opponent not in ['nan', '', 'None']:
                    opponent_stats[opponent]['picks'] += 1
                    if is_winner:
                        opponent_stats[opponent]['wins'] += 1
    
    # Best teammates (min 3 games together)
    best_teammates = []
    for teammate, data in teammates_stats.items():
        if data['picks'] >= 3:
            win_rate = (data['wins'] / data['picks'] * 100) if data['picks'] > 0 else 0
            best_teammates.append({
                'name': teammate,
                'picks': data['picks'],
                'wins': data['wins'],
                'win_rate': win_rate
            })
    best_teammates.sort(key=lambda x: x['win_rate'], reverse=True)
    
    # Calculate all matchups
    all_matchups = []
    for opponent, data in opponent_stats.items():
        if data['picks'] >= 5:
            win_rate = (data['wins'] / data['picks'] * 100) if data['picks'] > 0 else 0
            all_matchups.append({
                'name': opponent,
                'picks': data['picks'],
                'wins': data['wins'],
                'win_rate': win_rate
            })
    
    all_matchups.sort(key=lambda x: x['win_rate'], reverse=True)
    
    # Best matchups (top 10 highest win rates)
    best_matchups = all_matchups[:10]
    
    # Worst matchups (bottom 10 lowest win rates)
    worst_matchups = all_matchups[-10:]
    worst_matchups.reverse()
    
    overall_winrate = (brawler_mode_stats['wins'] / brawler_mode_stats['picks'] * 100) if brawler_mode_stats['picks'] > 0 else 0
    
    return render_template('brawler_mode.html',
                         user=session['discord_tag'],
                         brawler_name=brawler_name,
                         mode_name=mode_display,
                         stats=brawler_mode_stats,
                         overall_winrate=overall_winrate,
                         maps=maps,
                         best_teammates=best_teammates[:10],
                         best_matchups=best_matchups,
                         worst_matchups=worst_matchups)


@app.route('/brawler/<brawler_name>/map/<map_name>')
def brawler_map_page(brawler_name, map_name):
    """Brawler performance on a specific map"""
    matches_df, teams_data, _, _, _ = get_cached_data()
    
    if matches_df is None:
        return "Error loading data", 500
    
    # Convert URL format to display format
    map_display = map_name.replace('_', ' ').title()
    
    # Collect stats for this brawler on this map
    brawler_map_stats = {
        'picks': 0,
        'wins': 0,
        'mode': None
    }
    
    # Track teammates and opponents
    teammates_stats = defaultdict(lambda: {'picks': 0, 'wins': 0})
    opponent_stats = defaultdict(lambda: {'picks': 0, 'wins': 0})
    
    # Track series to avoid double-counting
    series_tracking = {}
    
    for _, match in matches_df.iterrows():
        match_map = str(match['map'])
        
        # Skip if not the right map
        if match_map.lower().replace(' ', '_').replace("'", '').replace('-', '_') != map_name.lower():
            continue
        
        map_display = match_map  # Use actual map name
        brawler_map_stats['mode'] = str(match['mode'])
        
        # Create series ID
        team1 = match['team1_name']
        team2 = match['team2_name']
        teams_sorted = tuple(sorted([team1, team2]))
        mode = str(match['mode'])
        
        team1_comp = sorted([
            str(match['team1_player1_brawler']),
            str(match['team1_player2_brawler']),
            str(match['team1_player3_brawler'])
        ])
        team2_comp = sorted([
            str(match['team2_player1_brawler']),
            str(match['team2_player2_brawler']),
            str(match['team2_player3_brawler'])
        ])
        
        comps_sorted = tuple(sorted([tuple(team1_comp), tuple(team2_comp)]))
        
        battle_time = match.get('battle_time')
        if pd.notna(battle_time):
            time_rounded = pd.Timestamp(battle_time).floor('30min')
        else:
            time_rounded = str(_)
        
        series_id = f"{teams_sorted}_{mode}_{match_map}_{comps_sorted}_{time_rounded}"
        
        if series_id not in series_tracking:
            series_tracking[series_id] = set()
        
        if brawler_name in series_tracking[series_id]:
            continue
        
        for team_prefix in ['team1', 'team2']:
            team_brawlers = [
                str(match[f'{team_prefix}_player1_brawler']),
                str(match[f'{team_prefix}_player2_brawler']),
                str(match[f'{team_prefix}_player3_brawler'])
            ]
            
            if brawler_name not in team_brawlers:
                continue
            
            series_tracking[series_id].add(brawler_name)
            
            # Update stats
            winner = str(match['winner'])
            team_name = match[f'{team_prefix}_name']
            is_winner = (winner == team_name)
            
            brawler_map_stats['picks'] += 1
            if is_winner:
                brawler_map_stats['wins'] += 1
            
            # Track teammates
            for teammate in team_brawlers:
                if teammate != brawler_name and teammate not in ['nan', '', 'None']:
                    teammates_stats[teammate]['picks'] += 1
                    if is_winner:
                        teammates_stats[teammate]['wins'] += 1
            
            # Track opponents
            enemy_prefix = 'team2' if team_prefix == 'team1' else 'team1'
            enemy_brawlers = [
                str(match[f'{enemy_prefix}_player1_brawler']),
                str(match[f'{enemy_prefix}_player2_brawler']),
                str(match[f'{enemy_prefix}_player3_brawler'])
            ]
            
            for opponent in enemy_brawlers:
                if opponent not in ['nan', '', 'None']:
                    opponent_stats[opponent]['picks'] += 1
                    if is_winner:
                        opponent_stats[opponent]['wins'] += 1
    
    if brawler_map_stats['picks'] == 0:
        return "No data available for this combination", 404
    
    # Calculate overall win rate
    overall_winrate = (brawler_map_stats['wins'] / brawler_map_stats['picks'] * 100) if brawler_map_stats['picks'] > 0 else 0
    
    # Best teammates (min 3 games together)
    best_teammates = []
    for teammate, data in teammates_stats.items():
        if data['picks'] >= 3:
            win_rate = (data['wins'] / data['picks'] * 100) if data['picks'] > 0 else 0
            best_teammates.append({
                'name': teammate,
                'picks': data['picks'],
                'wins': data['wins'],
                'win_rate': win_rate
            })
    best_teammates.sort(key=lambda x: x['win_rate'], reverse=True)
    
    # Calculate all matchups
    all_matchups = []
    for opponent, data in opponent_stats.items():
        if data['picks'] >= 5:
            win_rate = (data['wins'] / data['picks'] * 100) if data['picks'] > 0 else 0
            all_matchups.append({
                'name': opponent,
                'picks': data['picks'],
                'wins': data['wins'],
                'win_rate': win_rate
            })
    
    all_matchups.sort(key=lambda x: x['win_rate'], reverse=True)
    
    # Best matchups (top 10 highest win rates)
    best_matchups = all_matchups[:10]
    
    # Worst matchups (bottom 10 lowest win rates)
    worst_matchups = all_matchups[-10:]
    worst_matchups.reverse()
    
    return render_template('brawler_map.html',
                         user=session['discord_tag'],
                         brawler_name=brawler_name,
                         map_name=map_display,
                         mode_name=brawler_map_stats['mode'],
                         stats=brawler_map_stats,
                         overall_winrate=overall_winrate,
                         best_teammates=best_teammates[:10],
                         best_matchups=best_matchups,
                         worst_matchups=worst_matchups)

@app.route('/modes')
def modes_overview():
    """Overview page for all game modes"""
    _, teams_data, _, mode_stats, _ = get_cached_data()
    
    if not teams_data:
        return "Error loading data", 500
    
    # Collect comprehensive mode statistics
    modes_data = defaultdict(lambda: {
        'total_games': 0,
        'maps': set(),
        'brawlers': defaultdict(lambda: {'picks': 0, 'wins': 0})
    })
    
    for team_name, team in teams_data.items():
        for mode_name, mode_data in team['modes'].items():
            if mode_name in ['Unknown', 'nan', '', 'None']:
                continue
            
            # Count games
            modes_data[mode_name]['total_games'] += mode_data.get('matches', 0)
            
            # Track maps
            for map_name in mode_data.get('maps', {}).keys():
                modes_data[mode_name]['maps'].add(map_name)
            
            # Track brawler stats
            for map_name, map_data in mode_data.get('maps', {}).items():
                for brawler, brawler_data in map_data.get('brawlers', {}).items():
                    modes_data[mode_name]['brawlers'][brawler]['picks'] += brawler_data['picks']
                    modes_data[mode_name]['brawlers'][brawler]['wins'] += brawler_data['wins']
    
    # Build modes list with stats
    modes_list = []
    for mode_name, data in modes_data.items():
        # Find top brawler for this mode using meta score (win_rate * pick_rate)
        top_brawler = None
        if data['brawlers']:
            # Calculate total picks for this mode
            total_picks = sum(b['picks'] for b in data['brawlers'].values())
            
            # Calculate meta score for each brawler
            brawler_scores = []
            for brawler_name, brawler_data in data['brawlers'].items():
                if brawler_data['picks'] >= 5:  # Minimum 5 picks
                    win_rate = (brawler_data['wins'] / brawler_data['picks'] * 100)
                    pick_rate = (brawler_data['picks'] / total_picks * 100) if total_picks > 0 else 0
                    meta_score = win_rate * pick_rate  # win_rate * pick_rate
                    brawler_scores.append({
                        'name': brawler_name,
                        'win_rate': win_rate,
                        'picks': brawler_data['picks'],
                        'meta_score': meta_score
                    })
            
            # Get brawler with highest meta score
            if brawler_scores:
                top_brawler_data = max(brawler_scores, key=lambda x: x['meta_score'])
                top_brawler = {
                    'name': top_brawler_data['name'],
                    'win_rate': top_brawler_data['win_rate'],
                    'picks': top_brawler_data['picks']
                }
                print(f"Mode: {mode_name}, Top Brawler: {top_brawler}")  # Debug print
        
        modes_list.append({
            'name': mode_name,
            'total_games': data['total_games'],
            'total_maps': len(data['maps']),
            'total_brawlers': len(data['brawlers']),
            'top_brawler': top_brawler
        })
    
    # Sort by total games
    modes_list.sort(key=lambda x: x['total_games'], reverse=True)
    
    return render_template('modes_overview.html',
                         user=session['discord_tag'],
                         modes=modes_list)

@app.route('/team/<team_name>/brawler/<brawler_name>')
def team_brawler_page(team_name, brawler_name):
    _, teams_data, _, _, _ = get_cached_data()
    
    if team_name not in teams_data:
        return "Team not found", 404
    
    team = teams_data[team_name]
    
    if brawler_name not in team['brawlers']:
        return "Brawler not found", 404
    
    brawler_data = team['brawlers'][brawler_name]
    
    # Get mode stats for this brawler
    mode_stats = {}
    for mode, mode_data in team['modes'].items():
        for map_name, map_data in mode_data['maps'].items():
            if brawler_name in map_data['brawlers']:
                if mode not in mode_stats:
                    mode_stats[mode] = {'picks': 0, 'wins': 0}
                mode_stats[mode]['picks'] += map_data['brawlers'][brawler_name]['picks']
                mode_stats[mode]['wins'] += map_data['brawlers'][brawler_name]['wins']
    
    # Get player stats for this brawler
    player_stats = {}
    for player_tag, player_data in team['players'].items():
        if brawler_name in player_data['brawlers']:
            player_stats[player_tag] = {
                'name': player_data['name'],
                'picks': player_data['brawlers'][brawler_name]['picks'],
                'wins': player_data['brawlers'][brawler_name]['wins']
            }
    
    return render_template('team_brawler.html',
                         user=session['discord_tag'],
                         team_name=team_name,
                         team=team,
                         brawler_name=brawler_name,
                         brawler_data=brawler_data,
                         mode_stats=mode_stats,
                         player_stats=player_stats)

@app.route('/auth')
def auth():
    token = request.args.get('token')
    if not token:
        return render_template('login.html', error="No token provided")
    
    tokens = load_json(TOKENS_FILE)
    
    if token not in tokens:
        return render_template('login.html', error="Invalid token")
    
    token_data = tokens[token]
    
    if token_data.get('used', False):
        return render_template('login.html', error="Token already used")
    
    # Check if user is authorized
    if not is_user_authorized(token_data['discord_id']):
        return render_template('login.html', error="User not authorized")
    
    # Mark token as used
    tokens[token]['used'] = True
    save_json(TOKENS_FILE, tokens)
    
    # Create session
    session['discord_id'] = token_data['discord_id']
    session['discord_tag'] = token_data['discord_tag']
    
    return redirect('/dashboard')

if __name__ == '__main__':
    
    app.run(host='0.0.0.0', port=8080, debug=False)