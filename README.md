# Price Drop Tracker

Watches things you've already bought and tells you when the price drops while you can still claim the difference back. Built on the [TinyFish](https://tinyfish.ai) Agent API, which drives a real browser for each step.

## How it works

1. **Scan orders**: logs in to each retailer through a saved TinyFish browser profile and reads recent orders.
2. **Check prices**: opens every tracked product page in parallel and reads the current price.
3. **Compare**: checks the price you paid against today's price, for items still inside the retailer's price-match window.
4. **Notify**: emails you an alert with a ready-to-send claim. It can also send the claim to the retailer itself (`AUTO_SEND_CLAIMS`, off by default).

Results are stored in `tracker_state.json`, so an item only triggers a new alert if its price falls below the last one you were alerted about.

## Setup

Requires Python 3.10+.

```bash
pip install -r requirements.txt
cp .env.example .env   # then fill it in
```

| Variable | What it is |
| --- | --- |
| `TINYFISH_API_KEY` | TinyFish API key |
| `MY_NAME` | Name used to sign claim emails |
| `MY_EMAIL` | Where alerts are sent |
| `SMTP_USER` / `SMTP_APP_PASSWORD` | Gmail address and [app password](https://support.google.com/accounts/answer/185833) used to send email |
| `WOB_PROFILE_ID` | TinyFish browser profile that is signed in to World of Books |

To add retailers, add entries to `RETAILERS` in `main.py`. Other settings (minimum drop, auto-send, proxy) are at the top of the same file.

## Usage

```bash
python3 main.py              # one full run; schedule it daily with cron or Task Scheduler
python3 main.py --simulate   # fake price drop: no TinyFish runs, mock claim goes to MY_EMAIL only
python3 gui.py               # local dashboard at http://127.0.0.1:8765
```

The dashboard shows the pipeline stage, a live log, tracked items and alerts, and has buttons for a real run and a simulation.
