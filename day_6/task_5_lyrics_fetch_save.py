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

# I am going to use lrclib for fetching lyrics of all songs of currently playing artist.
# https://lrclib.net/docs
# Then log it.

import urllib.request
import urllib.parse
import json
import sqlite3
import time
import sys
from datetime import datetime

import pandas as pd

# ====================== CONFIG ======================
DB_FILE = "lyrics_database.db"
CSV_FILE = "lyrics_export.csv"
ARTIST_TRACK_LIMIT = 25

HEADERS = {
    "User-Agent": "LyricsETLScript/1.0 (Linux)"
}
# ===================================================

def urlopen_get(url, params=None, timeout=12):
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        print(f"HTTP Error {e.code}: {e.reason}")
        return None
    except Exception as e:
        print(f"Request error: {e}")
        return None


# ====================== MPRIS ======================
def get_current_song_info():
    try:
        import dbus
        session_bus = dbus.SessionBus()
        players = [name for name in session_bus.list_names() 
                  if name.startswith("org.mpris.MediaPlayer2.")]

        for player_name in players:
            try:
                player = session_bus.get_object(player_name, "/org/mpris/MediaPlayer2")
                properties = dbus.Interface(player, "org.freedesktop.DBus.Properties")

                metadata = properties.Get("org.mpris.MediaPlayer2.Player", "Metadata")
                status = properties.Get("org.mpris.MediaPlayer2.Player", "PlaybackStatus")

                if status != "Playing":
                    continue

                title = str(metadata.get("xesam:title", "Unknown"))
                artist = str(metadata.get("xesam:artist", ["Unknown"])[0])
                album = str(metadata.get("xesam:album", ""))

                duration = None
                if "mpris:length" in metadata:
                    duration = int(metadata["mpris:length"] / 1_000_000)

                print(f"✓ Detected via: {player_name.split('.')[-1]}")
                return {
                    "title": title,
                    "artist": artist,
                    "album": album,
                    "duration": duration,
                    "source": "MPRIS"
                }
            except:
                continue
        return None
    except ImportError:
        print("❌ pip install dbus-python")
        sys.exit(1)
    except Exception as e:
        print(f"MPRIS Error: {e}")
        return None


# ====================== LYRICS ======================
def search_lyrics(track_name, artist_name, album_name="", duration=None):
    params = {
        "track_name": track_name,
        "artist_name": artist_name,
        "album_name": album_name or "",
    }
    if duration:
        params["duration"] = int(duration)
    return urlopen_get("https://lrclib.net/api/get", params)


def search_songs_by_artist(artist_name, limit=ARTIST_TRACK_LIMIT):
    print(f"Searching for songs by '{artist_name}'...")
    params = { "artist_name": artist_name}
    
    data = urlopen_get("https://lrclib.net/api/search", params)
    
    if not isinstance(data, list) or len(data) == 0:
        print("   No tracks found.")
        return []

    print(f"✓ Found {len(data)} track(s):")
    for i, track in enumerate(data[:limit], 1):
        title = track.get("trackName", "N/A")
        album = track.get("albumName", "N/A")
        print(f"   {i:2d}. {title} — {album}")
    
    return data[:limit]


# ====================== DATABASE ======================
def init_database():
    conn = sqlite3.connect(DB_FILE)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS lyrics (
            lrclib_id INTEGER PRIMARY KEY,
            track_name TEXT,
            artist_name TEXT,
            album_name TEXT,
            duration INTEGER,
            instrumental BOOLEAN,
            plain_lyrics TEXT,
            synced_lyrics TEXT,
            fetched_at TEXT
        );

        CREATE TABLE IF NOT EXISTS play_log (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            lrclib_id INTEGER,
            track_name TEXT,
            artist_name TEXT,
            played_at TEXT,
            FOREIGN KEY (lrclib_id) REFERENCES lyrics(lrclib_id)
        );

        CREATE INDEX IF NOT EXISTS idx_play_log_date ON play_log(played_at);
        CREATE INDEX IF NOT EXISTS idx_play_log_song ON play_log(lrclib_id);
    """)
    conn.commit()
    conn.close()


def log_play(lrclib_id, track_name, artist_name):
    """Log every time a song is detected as playing"""
    conn = sqlite3.connect(DB_FILE)
    conn.execute("""
        INSERT INTO play_log (lrclib_id, track_name, artist_name, played_at)
        VALUES (?, ?, ?, ?)
    """, (lrclib_id, track_name, artist_name, datetime.now().isoformat()))
    conn.commit()
    conn.close()


def save_lyrics(record):
    """Save or update lyrics (no duplicates by lrclib_id)"""
    if not record or not record.get("id"):
        return False

    conn = sqlite3.connect(DB_FILE)
    try:
        conn.execute("""
            INSERT INTO lyrics 
            (lrclib_id, track_name, artist_name, album_name, duration, 
             instrumental, plain_lyrics, synced_lyrics, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(lrclib_id) DO UPDATE SET
                fetched_at = excluded.fetched_at,
                plain_lyrics = COALESCE(excluded.plain_lyrics, plain_lyrics),
                synced_lyrics = COALESCE(excluded.synced_lyrics, synced_lyrics)
        """, (
            record.get("id"),
            record.get("trackName"),
            record.get("artistName"),
            record.get("albumName"),
            record.get("duration"),
            record.get("instrumental", False),
            record.get("plainLyrics"),
            record.get("syncedLyrics"),
            datetime.now().isoformat()
        ))
        conn.commit()
        return True
    except Exception as e:
        print(f"Save error: {e}")
        return False
    finally:
        conn.close()


# ====================== MAIN ======================
def main():
    print("=== Linux Lyrics ETL + Play Logger ===\n")
    init_database()

    media = get_current_song_info()
    if not media:
        print("\nManual input:")
        title = input("Song Title : ").strip()
        artist = input("Artist     : ").strip()
        if not title or not artist:
            sys.exit(1)
        media = {"title": title, "artist": artist, "album": "", "duration": None}

    print(f"\nNow Playing → {media['title']} by {media['artist']}\n")

    all_records = []
    current_lrclib_id = None

    # 1. Current Song
    print("Fetching lyrics for current song...")
    current = search_lyrics(media["title"], media["artist"], media.get("album"), media.get("duration"))
    
    if current and isinstance(current, dict):
        if save_lyrics(current):
            current_lrclib_id = current.get("id")
            all_records.append(current)
            print("✓ Current song saved")
            log_play(current_lrclib_id, media["title"], media["artist"])

    # 2. Artist Songs
    artist_tracks = search_songs_by_artist(media["artist"])

    if artist_tracks:
        print(f"\nFetching lyrics for other tracks...")
        for track in artist_tracks:
            t_name = track.get("trackName")
            a_name = track.get("artistName")

            if t_name == media["title"] and a_name == media["artist"]:
                continue

            lyrics = search_lyrics(t_name, a_name, track.get("albumName", ""), track.get("duration"))
            if lyrics and isinstance(lyrics, dict):
                save_lyrics(lyrics)
                all_records.append(lyrics)
                print(f"   ✓ Saved: {t_name}")

            time.sleep(0.7)

    # Final Summary
    if all_records:
        df = pd.DataFrame(all_records)
        df = df.drop_duplicates(subset=['id'])
        
        print(f"\nTotal unique tracks processed: {len(df)}")
        print(df[['trackName', 'artistName', 'plainLyrics']].head().to_string(index=False))

        df.to_csv(CSV_FILE, index=False, encoding='utf-8')
        print(f"\n✅ Exported to CSV: {CSV_FILE}")
        print(f"📊 Play history is being logged in 'play_log' table for frequency analysis.")
    else:
        print("No new lyrics fetched.")


if __name__ == "__main__":
    main()