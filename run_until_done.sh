#!/bin/bash
# Runs the pipeline repeatedly until Harvard is fully harvested.
# Waits 30 minutes between restarts to let IP blocks lift.

source env/bin/activate
export PROXY_LIST="socks5h://127.0.0.1:9050"

WAIT=300    # 5 minutes between restarts

while true; do
    echo "$(date '+%Y-%m-%d %H:%M:%S') — Starting pipeline..."
    python qdarchive_pipeline.py --sources harvard

    # Check if all queries are done
    REMAINING=$(python3 -c "
import json
try:
    with open('metadata.progress.json') as f:
        p = json.load(f)
    not_done = [k for k,v in p.items() if v != 'done' and not str(k).startswith('columbia')]
    print(len(not_done))
except:
    print(1)
")
    if [ "$REMAINING" -eq 0 ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') — Harvard harvest complete!"
        break
    fi

    echo "$(date '+%Y-%m-%d %H:%M:%S') — Stopped with $REMAINING queries remaining. Waiting for Harvard to unblock..."
    until python3 -c "
import requests, sys
try:
    r = requests.get('https://dataverse.harvard.edu/api/search',
        params={'q':'test','type':'dataset','per_page':1},
        timeout=10)
    sys.exit(0 if r.status_code == 200 else 1)
except: sys.exit(1)
" 2>/dev/null; do
        echo "$(date '+%Y-%m-%d %H:%M:%S') — Still blocked, checking again in 2 min..."
        sleep 120
    done
    echo "$(date '+%Y-%m-%d %H:%M:%S') — Harvard is accessible again, restarting..."
done
