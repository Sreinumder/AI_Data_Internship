# Task 05 · Full ETL System [Hard — Month 1 Capstone]
# Bring together everything from Week 1 2 3 4 in one complete automated pipeline→ → →
# Goal Build a fully automated, reusable ETL pipeline that you could run every day to get fresh data.
# Week 1 File handling
# Week 2 API + requests
# Week 3 SQLite
# Week 4 Pandas + ETL
# Must EXTRACT from at least 1 real public API with full error handling
# Must Load into Pandas — clean nulls, duplicates, types, and string issues
# Must TRANSFORM — add at least 2 calculated/enriched columns
# Must LOAD to SQLite using df.to_sql() AND export 

# I am going to use lrclib for fetching lyrics of currently playing artist.
# then try fetching all songs by this artist.
# https://lrclib.net/docs
# Then log it.

import urllib.request
import urllib.parse
import json
import sqlite3
import time
import sys
from datetime import datetime
import signal
import pandas as pd
import dbus

DB_FILE = "lyrics_database.db"
CSV_FILE = "lyrics_export.csv"
MAX_SONGS = 60
CHECK_INTERVAL = 10  # seconds

current_song = None

def init_database():
    conn = sqlite3.connect(DB_FILE)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS lyrics (
            lrclib_id INTEGER PRIMARY KEY,
            track_name TEXT,
            artist_name TEXT,
            album_name TEXT,
            duration INTEGER,
            plain_lyrics TEXT,
            synced_lyrics TEXT,
            fetched_at TEXT
        );
    """)
    conn.commit()
    conn.close()


def urlopen_get(url, params=None):
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    
    headers = {"User-Agent": "LyricsETLScript/1.0"}
    req = urllib.request.Request(url, headers=headers)
    
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except:
        return None


def get_current_song_info():
    try:
        session_bus = dbus.SessionBus()
        players = [n for n in session_bus.list_names() if n.startswith("org.mpris.MediaPlayer2.")]

        for name in players:
            try:
                player = session_bus.get_object(name, "/org/mpris/MediaPlayer2")
                props = dbus.Interface(player, "org.freedesktop.DBus.Properties")
                
                metadata = props.Get("org.mpris.MediaPlayer2.Player", "Metadata")
                status = props.Get("org.mpris.MediaPlayer2.Player", "PlaybackStatus")

                if status == "Playing":
                    title = str(metadata.get("xesam:title", "Unknown"))
                    artist = str(metadata.get("xesam:artist", ["Unknown"])[0])
                    return {"title": title, "artist": artist}
            except:
                continue
        return None
    except:
        return None


def fetch_lyrics(track_name, artist_name):
    return urlopen_get("https://lrclib.net/api/get", {
        "track_name": track_name,
        "artist_name": artist_name
    })


def process_artist(artist_name):
    print(f"\nSearching songs by: {artist_name}")
    
    tracks = urlopen_get("https://lrclib.net/api/search", {
        "q": artist_name,
        "artist_name": artist_name
    })

    if not tracks or not isinstance(tracks, list):
        print("No tracks found for artist.")
        return

    # Deduplicate
    seen = set()
    filtered = []
    for t in tracks:
        title = t.get("trackName", "").strip()
        artist = t.get("artistName", "").strip()
        if title and artist.lower() == artist_name.lower():
            key = title.lower()
            if key not in seen:
                seen.add(key)
                filtered.append(t)

    print(f"Found {len(filtered)} unique songs. Fetching lyrics...\n")

    lyrics_list = []
    for track in filtered[:MAX_SONGS]:
        title = track.get("trackName")
        print(f"→ {title}")
        lyrics = fetch_lyrics(title, artist_name)
        
        if lyrics and isinstance(lyrics, dict):
            lyrics_list.append(lyrics)
            print("    Lyrics fetched")
        else:
            print("    No lyrics")
        
        time.sleep(0.7)

    if lyrics_list:
        df = pd.DataFrame(lyrics_list)
        df = df.drop_duplicates(subset=['id'])
        df.to_csv(CSV_FILE, index=False, encoding='utf-8')

        conn = sqlite3.connect(DB_FILE)
        for item in lyrics_list:
            conn.execute("""
                INSERT OR REPLACE INTO lyrics 
                (lrclib_id, track_name, artist_name, album_name, duration, 
                 plain_lyrics, synced_lyrics, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                item.get("id"),
                item.get("trackName"),
                item.get("artistName"),
                item.get("albumName"),
                item.get("duration"),
                item.get("plainLyrics"),
                item.get("syncedLyrics"),
                datetime.now().isoformat()
            ))
        conn.commit()
        conn.close()

        print(f"\n Saved {len(lyrics_list)} songs to database & CSV")


def signal_handler(sig, frame):
    print("\n\nbye :)")
    print("Program terminated by user.")
    sys.exit(0)


def main():
    print("=== lrclib Auto Lyrics Fetcher (Running) ===")
    print("Press Ctrl+C to stop.\n")
    init_database()
    signal.signal(signal.SIGINT, signal_handler)

    global current_song

    while True:
        media = get_current_song_info()

        if media:
            title = media["title"]
            artist = media["artist"]

            # Print current song lyrics if changed
            if current_song != title:
                print(f"\nNow Playing: {title} by {artist}")
                current_lyrics = fetch_lyrics(title, artist)
                
                if current_lyrics and isinstance(current_lyrics, dict):
                    plain = current_lyrics.get("plainLyrics", "No lyrics available.")
                    print("\n" + "="*60)
                    print(plain)
                    print("="*60 + "\n")
                else:
                    print("No lyrics found for current song.\n")

                # Fetch all songs by artist
                process_artist(artist)
                current_song = title

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()