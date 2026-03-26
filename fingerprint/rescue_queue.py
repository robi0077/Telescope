import os
import json
import redis

def rescue_temp_folders():
    temp_dir = r"C:\projects\scrapper\temp"
    redis_client = redis.Redis.from_url("redis://localhost:6666/0", decode_responses=True)
    queue = "queue:to_fingerprint"
    
    if not os.path.exists(temp_dir):
        print(f"Temp dir {temp_dir} not found.")
        return
        
    folders = [f for f in os.listdir(temp_dir) if os.path.isdir(os.path.join(temp_dir, f))]
    print(f"Found {len(folders)} folders in temp. Recovering...")
    
    count = 0
    for folder in folders:
        meta_path = os.path.join(temp_dir, folder, "metadata.json")
        if os.path.exists(meta_path):
            with open(meta_path, 'r', encoding='utf-8') as mf:
                try:
                    meta = json.load(mf)
                except Exception:
                    continue
                
            envelope = {
                "task_id": folder,
                "site_name": meta.get("site_name", "rescued"),
                "original_url": meta.get("link", "rescued"),
                "video_folder": folder,
                # Enriched Metadata mapping from the old scraper format
                "duration": meta.get("duration"),
                "resolution": meta.get("quality"),
                "title": meta.get("title", f"Rescued Video {folder}"),
            }
            
            redis_client.rpush(queue, json.dumps(envelope))
            count += 1
            print(f"Queued {folder}")
            
    print(f"Successfully pushed {count} stranded videos to the queue!")

if __name__ == "__main__":
    rescue_temp_folders()
