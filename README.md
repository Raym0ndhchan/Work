# AISStream configurable capture (Python)

This project provides a **manual-run** Python script to capture AISStream data and write it to **both JSONL and CSV**.

## 1) Setup

Use Python 3.8+ (recommended: `python3`).

1. Install dependency:
   ```bash
   pip install -r requirements.txt
   ```
2. Set your AISStream API key:
   ```bash
   export AISSTREAM_API_KEY="your_api_key"
   ```
3. Create your config file:
   ```bash
   cp config.json.example config.json
   ```

## 2) Configure capture settings

Edit `config.json`:

- `capture_minutes`: capture duration (e.g. `5`)
- `bounding_boxes`: target region(s) as AISStream bounding boxes
  - Global: `[[[-90, -180], [90, 180]]]`
  - Example (US East Coast rough box): `[[[24.0, -82.0], [46.0, -66.0]]]`
- `output_jsonl`: path to JSONL output file
- `output_csv`: path to CSV output file

## 3) Run (only when you want)

```bash
python3 capture_global_ais.py --config config.json
```

The script does **not** run automatically. It only captures data when you execute the command above.

## Output formats

- JSONL: one raw AIS message per line.
- CSV: flattened summary columns (`received_at`, `message_type`, `mmsi`, `latitude`, `longitude`, `sog`, `cog`, `heading`, `ship_name`, `raw_json`).

## Troubleshooting

If you see:

```
IndentationError: expected an indented block after function definition
```

it usually means your local `capture_global_ais.py` still contains a bad merge edit (for example, duplicate `def normalize_message_payload(...)` lines).

Fix quickly by refreshing from git and re-running:

```bash
git checkout -- capture_global_ais.py
python3 -m py_compile capture_global_ais.py
python3 capture_global_ais.py --config config.json
```

If you manually resolved conflicts in GitHub UI, make sure there is only **one** `normalize_message_payload` function definition and no conflict markers (`<<<<<<<`, `=======`, `>>>>>>>`).

Security note: if you accidentally posted your `AISSTREAM_API_KEY` in a public place, revoke it in AISStream and generate a new key immediately.


If you see `WebSocketConnectionClosedException: Connection to remote host was lost`, the script now retries automatically during the capture window. Intermittent drops can still happen on unstable networks; keep the terminal open and let it continue reconnecting.
