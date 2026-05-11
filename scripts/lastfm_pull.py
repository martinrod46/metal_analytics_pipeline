import requests
import pandas as pd
import time
import os
from dotenv import load_dotenv
 
# ─────────────────────────────────────
# LOAD ENV VARIABLES
# ─────────────────────────────────────
load_dotenv()
 
LASTFM_API_KEY = os.getenv("LASTFM_API_KEY")
 
if not LASTFM_API_KEY:
    raise ValueError("❌ LASTFM_API_KEY not found in .env file")
 
print("✅ Last.fm API key loaded!")
 
# ─────────────────────────────────────
# LAST.FM BASE URL
# ─────────────────────────────────────
BASE_URL = "http://ws.audioscrobbler.com/2.0/"
 
# ─────────────────────────────────────
# PERSISTENT SESSION
# ─────────────────────────────────────
session = requests.Session()
session.headers.update({
    "User-Agent": "metal-analytics-pipeline/1.0"
})
 
# ─────────────────────────────────────
# SAFE REQUEST FUNCTION WITH RETRY
# ─────────────────────────────────────
def lastfm_get(params, retries=3):
    for attempt in range(retries):
        try:
            response = session.get(
                BASE_URL,
                params=params,
                timeout=60
            )
 
            # SUCCESS
            if response.status_code == 200:
                return response
 
            # RATE LIMIT
            elif response.status_code == 429:
                wait = 5 * (attempt + 1)
                print(f"  ⏳ Rate limited — waiting {wait}s...")
                time.sleep(wait)
 
            # OTHER ERRORS
            else:
                print(f"  ⚠️ Error {response.status_code}")
                try:
                    print(response.json())
                except:
                    print(response.text[:200])
                return response
 
        # CONNECTION ERRORS
        except requests.exceptions.ConnectionError:
            wait = 5 * (attempt + 1)
            print(f"  ⚠️ Connection error (attempt {attempt+1}/{retries}) — retrying in {wait}s...")
            time.sleep(wait)
 
        # TIMEOUTS
        except requests.exceptions.Timeout:
            wait = 5 * (attempt + 1)
            print(f"  ⚠️ Timeout (attempt {attempt+1}/{retries}) — retrying in {wait}s...")
            time.sleep(wait)
 
        # GENERIC REQUEST ERROR
        except requests.exceptions.RequestException as e:
            print(f"  ⚠️ Request failed: {e}")
            return None
 
    print("  ❌ All retries failed")
    return None
 
# ─────────────────────────────────────
# METAL GENRE TAGS — 30 subgenres
# ─────────────────────────────────────
METAL_GENRES = [
    "heavy metal",
    "death metal",
    "black metal",
    "doom metal",
    "thrash metal",
    "new wave of british heavy metal",
    "power metal",
    "progressive metal",
    "grindcore",
    "metalcore",
    "nu metal",
    "melodic death metal",
    "sludge metal",
    "industrial metal",
    "stoner metal",
    "symphonic metal",
    "groove metal",
    "deathcore",
    "glam metal",
    "folk metal",
    "speed metal",
    "gothic metal",
    "post metal",
    "technical death metal",
    "death doom metal",
    "avant garde metal",
    "viking metal",
    "black n roll",
    "war metal",
    "brutal death metal"
]
 
# ─────────────────────────────────────
# STEP 1: FETCH TOP ARTISTS PER GENRE
# ─────────────────────────────────────
print("\n🔍 Fetching top artists per genre...")
 
genre_artist_map = {}
 
for genre in METAL_GENRES:
    params = {
        "method":  "tag.getTopArtists",
        "tag":     genre,
        "api_key": LASTFM_API_KEY,
        "format":  "json",
        "limit":   50
    }
 
    response = lastfm_get(params)
 
    if not response or response.status_code != 200:
        print(f"  ⚠️ Failed genre: {genre}")
        continue
 
    data    = response.json()
    artists = data.get("topartists", {}).get("artist", [])
 
    if not artists:
        print(f"  ⚠️ No artists found: {genre}")
        continue
 
    genre_artist_map[genre] = [a.get("name") for a in artists]
    print(f"  ✅ {genre} — {len(genre_artist_map[genre])} artists")
    time.sleep(1)
 
# ─────────────────────────────────────
# BUILD UNIQUE ARTIST LIST
# ─────────────────────────────────────
unique_artists = set()
 
for artists in genre_artist_map.values():
    unique_artists.update(artists)
 
unique_artists = sorted(unique_artists)
 
print(f"\n✅ Unique artists collected: {len(unique_artists)}")
 
# ─────────────────────────────────────
# HELPER — Get genres for an artist
# ─────────────────────────────────────
def get_artist_genres(artist_name):
    return ", ".join([
        genre for genre, artists
        in genre_artist_map.items()
        if artist_name in artists
    ])
 
# ─────────────────────────────────────
# STEP 2: GET TOP TRACKS
# ─────────────────────────────────────
all_tracks = []
 
print("\n🎵 Pulling top tracks...")
 
for artist in unique_artists:
    params = {
        "method":  "artist.gettoptracks",
        "artist":  artist,
        "api_key": LASTFM_API_KEY,
        "format":  "json",
        "limit":   10
    }
 
    response = lastfm_get(params)
 
    if not response or response.status_code != 200:
        print(f"  ⚠️ Failed tracks: {artist}")
        continue
 
    data   = response.json()
    tracks = data.get("toptracks", {}).get("track", [])
 
    if not tracks:
        print(f"  ⚠️ No tracks found: {artist}")
        continue
 
    for track in tracks:
        all_tracks.append({
            "artist_name": artist,
            "track_name":  track.get("name"),
            "listeners":   track.get("listeners"),
            "playcount":   track.get("playcount"),
            "genre":       get_artist_genres(artist)
        })
 
    print(f"  ✅ Tracks: {artist} — {len(tracks)} tracks")
    time.sleep(1)
 
# ─────────────────────────────────────
# TRACKS DATAFRAME
# ─────────────────────────────────────
df_tracks = pd.DataFrame(all_tracks)
 
if not df_tracks.empty:
    df_tracks["listeners"] = pd.to_numeric(df_tracks["listeners"], errors="coerce")
    df_tracks["playcount"] = pd.to_numeric(df_tracks["playcount"], errors="coerce")
    df_tracks = df_tracks.drop_duplicates()
 
print(f"\n✅ Tracks collected: {len(df_tracks)}")
 
# ─────────────────────────────────────
# STEP 3: GET ARTIST METADATA
# ─────────────────────────────────────
artist_metadata = []
 
print("\n🎤 Pulling artist metadata...")
 
for artist in unique_artists:
    params = {
        "method":  "artist.getinfo",
        "artist":  artist,
        "api_key": LASTFM_API_KEY,
        "format":  "json"
    }
 
    response = lastfm_get(params)
 
    if not response or response.status_code != 200:
        print(f"  ⚠️ Failed metadata: {artist}")
        continue
 
    data        = response.json()
    artist_data = data.get("artist", {})
    stats       = artist_data.get("stats", {})
    tags        = artist_data.get("tags", {}).get("tag", [])
    tag_names   = [tag.get("name") for tag in tags]
 
    artist_metadata.append({
        "artist_name": artist,
        "listeners":   stats.get("listeners"),
        "playcount":   stats.get("playcount"),
        "tags":        ", ".join(tag_names),
        "genre":       get_artist_genres(artist)
    })
 
    print(f"  ✅ Metadata: {artist}")
    time.sleep(1)
 
# ─────────────────────────────────────
# ARTISTS DATAFRAME
# ─────────────────────────────────────
df_artists = pd.DataFrame(artist_metadata)
 
if not df_artists.empty:
    df_artists["listeners"] = pd.to_numeric(df_artists["listeners"], errors="coerce")
    df_artists["playcount"] = pd.to_numeric(df_artists["playcount"], errors="coerce")
    df_artists = df_artists.drop_duplicates()
 
print(f"\n✅ Artist metadata collected: {len(df_artists)}")
 
# ─────────────────────────────────────
# STEP 4: GET SIMILAR ARTISTS
# ─────────────────────────────────────
similar_artists = []
 
print("\n🔗 Pulling similar artists...")
 
for artist in unique_artists:
    params = {
        "method":  "artist.getSimilar",
        "artist":  artist,
        "api_key": LASTFM_API_KEY,
        "format":  "json",
        "limit":   5
    }
 
    response = lastfm_get(params)
 
    if not response or response.status_code != 200:
        print(f"  ⚠️ Failed similar artists: {artist}")
        continue
 
    data    = response.json()
    similar = data.get("similarartists", {}).get("artist", [])
 
    for sim in similar:
        similar_artists.append({
            "artist_name":    artist,
            "similar_artist": sim.get("name"),
            "match_score":    sim.get("match"),
            "genre":          get_artist_genres(artist)
        })
 
    print(f"  ✅ Similar: {artist}")
    time.sleep(1)
 
# ─────────────────────────────────────
# SIMILAR ARTISTS DATAFRAME
# ─────────────────────────────────────
df_similar = pd.DataFrame(similar_artists)
 
if not df_similar.empty:
    df_similar["match_score"] = pd.to_numeric(df_similar["match_score"], errors="coerce")
    df_similar = df_similar.drop_duplicates()
 
print(f"\n✅ Similar artists collected: {len(df_similar)}")
 
# ─────────────────────────────────────
# STEP 5: GET TOP ALBUMS
# ─────────────────────────────────────
all_albums = []
 
print("\n💿 Pulling top albums...")
 
for artist in unique_artists:
    params = {
        "method":  "artist.getTopAlbums",
        "artist":  artist,
        "api_key": LASTFM_API_KEY,
        "format":  "json",
        "limit":   5
    }
 
    response = lastfm_get(params)
 
    if not response or response.status_code != 200:
        print(f"  ⚠️ Failed albums: {artist}")
        continue
 
    data   = response.json()
    albums = data.get("topalbums", {}).get("album", [])
 
    for album in albums:
        all_albums.append({
            "artist_name": artist,
            "album_name":  album.get("name"),
            "playcount":   album.get("playcount"),
            "genre":       get_artist_genres(artist)
        })
 
    print(f"  ✅ Albums: {artist}")
    time.sleep(1)
 
# ─────────────────────────────────────
# ALBUMS DATAFRAME
# ─────────────────────────────────────
df_albums = pd.DataFrame(all_albums)
 
if not df_albums.empty:
    df_albums["playcount"] = pd.to_numeric(df_albums["playcount"], errors="coerce")
    df_albums = df_albums.drop_duplicates()
 
print(f"\n✅ Albums collected: {len(df_albums)}")
 
# ─────────────────────────────────────
# SAVE ALL CSV FILES
# ─────────────────────────────────────
os.makedirs("data/raw", exist_ok=True)
 
df_tracks.to_csv("data/raw/lastfm_tracks.csv", index=False)
print("✅ Saved: data/raw/lastfm_tracks.csv")
 
df_artists.to_csv("data/raw/lastfm_artists.csv", index=False)
print("✅ Saved: data/raw/lastfm_artists.csv")
 
df_similar.to_csv("data/raw/lastfm_similar_artists.csv", index=False)
print("✅ Saved: data/raw/lastfm_similar_artists.csv")
 
df_albums.to_csv("data/raw/lastfm_albums.csv", index=False)
print("✅ Saved: data/raw/lastfm_albums.csv")
 
# ─────────────────────────────────────
# FINAL SUMMARY
# ─────────────────────────────────────
print("\n" + "="*50)
print("🤘 LAST.FM PIPELINE COMPLETED!")
print("="*50)
print(f"  Genres processed:    {len(genre_artist_map)}")
print(f"  Unique artists:      {len(unique_artists)}")
print(f"  Tracks collected:    {len(df_tracks)}")
print(f"  Artist metadata:     {len(df_artists)}")
print(f"  Similar artists:     {len(df_similar)}")
print(f"  Albums collected:    {len(df_albums)}")
print("="*50)
print("\n📦 Next step: Upload all CSVs to demo_volume/lastfm/ in Databricks!")