"""
Post-purchase price tracker built on the TinyFish Agent API.

Pipeline (run once a day from cron / Task Scheduler):
  1. SCAN ORDERS   -> one /automation/run per retailer, logged in via a Browser Context Profile
  2. CHECK PRICES  -> one /automation/run-async per tracked item, all in parallel, then poll /runs/{id}
  3. COMPARE       -> price paid vs current price, only for items still inside their price-match window
  4. NOTIFY        -> email you an alert with a ready-to-send claim; optionally send the claim itself

Env vars (see .env.example):
  TINYFISH_API_KEY, MY_NAME, MY_EMAIL, SMTP_USER, SMTP_APP_PASSWORD, WOB_PROFILE_ID

Usage:
  python3 main.py              # real run
  python3 main.py --simulate   # fake price drop, emails a mock claim to MY_EMAIL only
"""

import json
import os
import smtplib
import ssl
import sys
import time
from datetime import date, datetime, timedelta
from email.message import EmailMessage

import certifi
import requests
from dotenv import load_dotenv

HERE = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(HERE, ".env"))

# ----------------------------------------------------------------------------- config

API = "https://agent.tinyfish.ai/v1"
HEADERS = {"X-API-Key": os.environ["TINYFISH_API_KEY"], "Content-Type": "application/json"}

STATE_FILE = os.path.join(HERE, "tracker_state.json")
MIN_DROP_GBP = 2.00          # ignore tiny drops that aren't worth a claim
AUTO_SEND_CLAIMS = False     # start False: you review the claim before it goes out
POLL_SECONDS = 10

MY_NAME = os.environ["MY_NAME"]                # signs the claim emails
MY_EMAIL = os.environ["MY_EMAIL"]
SMTP_HOST, SMTP_PORT = "smtp.gmail.com", 465
SMTP_USER = os.environ["SMTP_USER"]
SMTP_PASS = os.environ["SMTP_APP_PASSWORD"]   # Gmail app password, not your real password

UK_PROXY = {"enabled": True, "type": "tinyfish", "country_code": "GB"}  # see UK prices, not US

RETAILERS = [
    {
        "name": "World of Books",
        "orders_url": "https://www.worldofbooks.com/en-gb/account",
        "claim_email": "customerservice@worldofbooks.com",
        "window_days": 14,              # no published price-match policy; this is a guess
        "profile_id": os.environ["WOB_PROFILE_ID"],  # TinyFish profile logged in to the site
    },
    # add more retailers here
]

# ----------------------------------------------------------------------------- schemas

ORDERS_SCHEMA = {
    "type": "object",
    "properties": {
        "logged_in": {"type": "boolean"},
        "account_name": {"type": "string"},       # name / email shown on the account page
        "total_orders_on_page": {"type": "integer"},
        "notes": {"type": "string"},              # anything that blocked the agent
        "orders": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "order_id": {"type": "string"},
                    "order_date": {"type": "string"},
                    "status": {"type": "string"},
                    "order_total": {"type": "number"},
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "product_url": {"type": "string"},
                                "price_paid": {"type": "number"},
                                "quantity": {"type": "integer"},
                                "condition": {"type": "string"},
                            },
                            "required": ["name", "product_url", "price_paid"],
                        },
                    },
                },
                "required": ["order_id", "order_date", "items"],
            },
        }
    },
    "required": ["logged_in", "orders"],
}

PRICE_SCHEMA = {
    "type": "object",
    "properties": {
        "current_price": {"type": "number"},
        "in_stock": {"type": "boolean"},
    },
    "required": ["current_price"],
}

# ----------------------------------------------------------------------------- logging

LISTENERS = []   # callbacks the GUI registers to follow along: fn(event: dict)


def emit(event: dict) -> None:
    for fn in LISTENERS:
        fn(event)


def log(msg: str) -> None:
    line = f"[{datetime.now():%H:%M:%S}] {msg}"
    print(line, flush=True)
    emit({"type": "log", "line": line})


def stage(name: str) -> None:
    """Pipeline stage: scan, check, compare, notify, done."""
    emit({"type": "stage", "stage": name})


def short(text: str, n: int = 60) -> str:
    return text if len(text) <= n else text[: n - 1] + "…"

# ----------------------------------------------------------------------------- TinyFish helpers

def run_sync(payload: dict) -> dict | None:
    """Blocking run. Good for the orders page: one call per retailer."""
    started = time.time()
    r = requests.post(f"{API}/automation/run", headers=HEADERS, json=payload, timeout=900)
    if not r.ok:
        log(f"  run rejected ({r.status_code}): {r.text}")
        return None
    data = r.json()
    log(f"  browser finished in {time.time() - started:.0f}s, status {data.get('status')}")
    if data.get("run_id"):
        log(f"  run id {data['run_id']}")
        show_steps(data["run_id"])
    # COMPLETED only means the browser finished; still validate the result itself
    if data.get("status") != "COMPLETED" or not data.get("result"):
        log(f"  run failed: {data.get('error')}")
        return None
    return data["result"]


def show_steps(run_id: str) -> None:
    """Print each action the browser agent took, so you can see what it actually did."""
    r = requests.get(f"{API}/runs/{run_id}", headers=HEADERS, timeout=30)
    if not r.ok:
        return
    for i, step in enumerate(r.json().get("steps") or [], 1):
        log(f"    step {i}: {step.get('action')}")


def profile_sites(profile_id: str) -> list[str] | None:
    """Domains this profile has saved cookies for (None if we couldn't check)."""
    r = requests.get(f"{API}/profiles/{profile_id}", headers=HEADERS, timeout=30)
    if not r.ok:
        return None
    return [f"{d['domain']} ({d.get('cookie_count', 0)} cookies, saved {d.get('last_written_at')})"
            for d in r.json().get("domains") or []]


def start_async(payload: dict) -> str | None:
    """Non-blocking run. Returns run_id immediately so many items can run in parallel."""
    r = requests.post(f"{API}/automation/run-async", headers=HEADERS, json=payload, timeout=60)
    if not r.ok:
        log(f"  could not start run ({r.status_code}): {r.text}")
        return None
    data = r.json()
    if data.get("error"):
        log(f"  could not start run: {data['error']}")
        return None
    return data["run_id"]


def wait_for(run_ids: dict[str, str], labels: dict[str, str] | None = None,
             timeout_s: int = 1200) -> dict[str, dict | None]:
    """Poll GET /runs/{id} until every run is terminal. run_ids maps item_key -> run_id."""
    labels = labels or {}
    results, pending = {}, dict(run_ids)
    total, started = len(run_ids), time.time()
    deadline = started + timeout_s
    while pending and time.time() < deadline:
        for key, run_id in list(pending.items()):
            r = requests.get(f"{API}/runs/{run_id}", headers=HEADERS, timeout=30)
            if r.status_code != 200:
                continue
            run = r.json()
            if run["status"] in ("COMPLETED", "FAILED", "CANCELLED"):
                label = labels.get(key, key)
                results[key] = run.get("result") if run["status"] == "COMPLETED" else None
                res = results[key]
                if run["status"] != "COMPLETED":
                    log(f"  ✗ {label}: {run['status']} – {run.get('error')}")
                elif res and res.get("current_price") is not None:
                    stock = {True: "in stock", False: "OUT OF STOCK"}.get(res.get("in_stock"), "stock unknown")
                    log(f"  ✓ {label}: found £{float(res['current_price']):.2f} ({stock})")
                else:
                    log(f"  ? {label}: finished but no price found – {res}")
                del pending[key]
        if pending:
            log(f"  …{total - len(pending)}/{total} done, waiting on {len(pending)} "
                f"({time.time() - started:.0f}s elapsed)")
            time.sleep(POLL_SECONDS)
    for key, run_id in pending.items():          # anything still running: cancel and skip
        log(f"  ✗ {labels.get(key, key)}: timed out after {timeout_s}s, cancelling run {run_id}")
        requests.post(f"{API}/runs/{run_id}/cancel", headers=HEADERS, timeout=30)
        results[key] = None
    return results

# ----------------------------------------------------------------------------- state

def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"items": {}, "alerted": {}}


def save_state(state: dict) -> None:
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def parse_date(s: str) -> date | None:
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def days_left(item: dict) -> int | None:
    """Days left in the item's price-match window (negative once expired, None if unknown)."""
    d = parse_date(item["order_date"])
    window = next((r["window_days"] for r in RETAILERS if r["name"] == item["retailer"]), None)
    return (d + timedelta(days=window) - date.today()).days if d and window else None

# ----------------------------------------------------------------------------- stage 1: orders

def scan_orders(retailer: dict, state: dict) -> None:
    log(f"Scanning orders: {retailer['name']}")
    log(f"  website: {retailer['orders_url']}")
    log(f"  profile: {retailer['profile_id']}, looking back {retailer['window_days']} days")
    retailer_domain = retailer["orders_url"].split("/")[2].removeprefix("www.")
    sites = profile_sites(retailer["profile_id"])
    if sites is not None:
        site = next((x for x in sites if retailer_domain in x), None)
        log(f"  profile has saved cookies for this site: {site or 'NO'}")
    log("  waiting for the browser (this can take a few minutes)…")
    result = run_sync({
        "url": retailer["orders_url"],
        "goal": (
            "First check whether you are logged in. If you see a login or sign-in form, log in only with "
            "saved Vault credentials if they are available; never guess or type any other credentials. "
            "If you still cannot get in, set logged_in to false, explain in notes, and return an empty "
            "orders list. "
            "If logged in, set logged_in to true and account_name to the name or email shown for the "
            "account. Open the order history and set total_orders_on_page to how many orders it lists. "
            f"For every order placed in the last {retailer['window_days']} days, return the order ID, "
            "the order date as YYYY-MM-DD, the order status, the order total in GBP, and for each item: "
            "the product name, the full URL of the item's product page, the price paid per unit in GBP "
            "as a number, the quantity, and the condition if shown. "
            "Read only. Do not click anything that cancels, returns, reorders or changes an order."
        ),
        "output_schema": ORDERS_SCHEMA,
        "use_profile": True,
        "use_vault": True,              # if the saved session expired, re-login with Vault credentials
        "profile_id": retailer["profile_id"],
        "proxy_config": UK_PROXY,
    })
    if not result:
        log(f"  no orders read from {retailer['name']}")
        return
    if result.get("notes"):
        log(f"  agent notes: {result['notes']}")
    if not result.get("logged_in"):
        log(f"  ✗ NOT LOGGED IN to {retailer['name']}. Sign in to the site inside TinyFish profile "
            f"{retailer['profile_id']}, then run again.")
        return
    log(f"  ✓ logged in as {result.get('account_name') or '(name not shown)'}")
    orders = result.get("orders", [])
    on_page = result.get("total_orders_on_page")
    log(f"  order history lists {on_page if on_page is not None else '?'} order(s); "
        f"{len(orders)} placed in the last {retailer['window_days']} days")
    for order in orders:
        total = order.get("order_total")
        log(f"  order {order['order_id']} placed {order['order_date']}"
            f" | {order.get('status') or 'status ?'}"
            f" | total {f'£{float(total):.2f}' if total is not None else '?'}"
            f" | {len(order.get('items', []))} item(s)")
        for item in order.get("items", []):
            key = f"{retailer['name']}|{order['order_id']}|{item['product_url']}"
            tag = "already tracked" if key in state["items"] else "NEW"
            extra = ", ".join(x for x in (
                f"qty {item['quantity']}" if item.get("quantity") else "",
                item.get("condition") or "") if x)
            log(f"    - {short(item['name'])}: paid £{float(item['price_paid']):.2f}"
                f"{f' ({extra})' if extra else ''} [{tag}]")
            log(f"      {item['product_url']}")
            state["items"].setdefault(key, {
                "retailer": retailer["name"],
                "order_id": order["order_id"],
                "order_date": order["order_date"],
                "name": item["name"],
                "product_url": item["product_url"],
                "price_paid": float(item["price_paid"]),
            })

# ----------------------------------------------------------------------------- stage 2: prices

def active_items(state: dict) -> dict:
    """Items still inside their retailer's price-match window."""
    active = {}
    for key, item in state["items"].items():
        left = days_left(item)
        if left is not None and left >= 0:
            active[key] = item
            log(f"  tracking {short(item['name'])} – {left} day(s) left in window")
        else:
            reason = "window expired" if left is not None else "unreadable order date or retailer removed"
            log(f"  skipping {short(item['name'])} – {reason}")
    return active


def check_prices(items: dict) -> dict[str, dict | None]:
    log(f"Checking {len(items)} item price(s) in parallel")
    run_ids, labels = {}, {}
    for key, item in items.items():
        labels[key] = f"{short(item['name'], 45)} ({item['retailer']})"
        log(f"  starting check: {labels[key]}")
        log(f"    {item['product_url']}")
        run_id = start_async({
            "url": item["product_url"],
            "goal": (
                f"Read the current selling price in GBP of this product ('{item['name']}') as a number. "
                "Use the standard price for the same variant. Ignore loyalty-card or member-only prices, "
                "delivery costs, and offers from other sellers. Report whether it is in stock."
            ),
            "output_schema": PRICE_SCHEMA,
            "proxy_config": UK_PROXY,
        })
        if run_id:
            run_ids[key] = run_id
            log(f"    run id {run_id}")
    return wait_for(run_ids, labels)

# ----------------------------------------------------------------------------- stage 3 & 4

def send_email(to: str, subject: str, body: str, cc: str | None = None) -> None:
    msg = EmailMessage()
    msg["From"], msg["To"], msg["Subject"] = SMTP_USER, to, subject
    if cc:
        msg["Cc"] = cc
    msg.set_content(body)
    # certifi's CA bundle: python.org builds on macOS don't see the system certificates
    context = ssl.create_default_context(cafile=certifi.where())
    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=context) as s:
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)


def claim_text(item: dict, current: float) -> str:
    return (
        f"Hello,\n\n"
        f"I bought {item['name']} on {item['order_date']} (order {item['order_id']}) for "
        f"£{item['price_paid']:.2f}. It is now listed on your website at £{current:.2f}:\n"
        f"{item['product_url']}\n\n"
        f"As this is within your price-match period, I'd like to request a refund of the "
        f"£{item['price_paid'] - current:.2f} difference to my original payment method.\n\n"
        f"Many thanks,\n{MY_NAME}\n"
    )


def process_drops(items: dict, prices: dict, state: dict) -> None:
    log("Comparing prices")
    claim_emails = {r["name"]: r["claim_email"] for r in RETAILERS}
    for key, item in items.items():
        name = short(item["name"], 45)
        res = prices.get(key)
        if not res or res.get("current_price") is None:
            log(f"  {name}: no price, skipped")
            continue
        current, paid = float(res["current_price"]), item["price_paid"]
        state.setdefault("last_checked", {})[key] = {
            "price": current, "in_stock": res.get("in_stock"), "at": datetime.now().isoformat(timespec="seconds")}
        drop = paid - current
        last_alerted = state["alerted"].get(key)
        line = f"  {name}: paid £{paid:.2f}, now £{current:.2f}"
        if drop <= 0:
            log(f"{line} – no drop")
            continue
        if drop < MIN_DROP_GBP:
            log(f"{line} – dropped £{drop:.2f}, under the £{MIN_DROP_GBP:.2f} minimum")
            continue
        if last_alerted is not None and current >= last_alerted:
            log(f"{line} – already alerted at £{last_alerted:.2f}")
            continue
        stage("notify")
        claim = claim_text(item, current)
        to = claim_emails[item["retailer"]]
        if AUTO_SEND_CLAIMS:
            send_email(to, f"Price adjustment request – order {item['order_id']}", claim, cc=MY_EMAIL)
            status = f"Claim SENT to {to} (you were CC'd)."
        else:
            status = f"Claim NOT sent. Review it and send to {to}."
        send_email(
            MY_EMAIL,
            f"Price drop: {item['name']} – £{drop:.2f} back",
            f"{item['name']} ({item['retailer']})\nPaid £{paid:.2f}, now £{current:.2f}.\n"
            f"{status}\n\n----- claim -----\n{claim}",
        )
        state["alerted"][key] = current
        emit({"type": "alert", "name": item["name"], "retailer": item["retailer"], "paid": paid,
              "now": current, "drop": drop, "to": to, "sent": AUTO_SEND_CLAIMS, "simulated": False})
        log(f"{line} – ALERT: £{drop:.2f} drop, emailed {MY_EMAIL}. {status}")


def main() -> None:
    state = load_state()
    log(f"Starting: {len(RETAILERS)} retailer(s), {len(state['items'])} item(s) already tracked")
    stage("scan")
    for retailer in RETAILERS:
        scan_orders(retailer, state)
    save_state(state)                 # save after scanning so a later crash doesn't lose orders

    log("Finding items still inside their price-match window")
    items = active_items(state)
    if items:
        stage("check")
        prices = check_prices(items)
        stage("compare")
        process_drops(items, prices, state)
    else:
        log("  nothing to check")
    save_state(state)
    stage("done")
    log(f"Done. State saved to {STATE_FILE}")

# ----------------------------------------------------------------------------- simulation

SAMPLE_ITEM = {
    "retailer": RETAILERS[0]["name"],
    "order_id": "SIM-0001",
    "order_date": date.today().isoformat(),
    "name": "The Hobbit (Paperback, J.R.R. Tolkien)",
    "product_url": "https://www.worldofbooks.com/en-gb/products/the-hobbit-book-j-r-r-tolkien-9780261102217",
    "price_paid": 8.99,
}


def simulate_price_drop(drop: float = 3.50) -> None:
    """Walk through the whole pipeline with a fake price drop. No TinyFish runs, no state changes.
    The claim that would go to customer service is emailed to MY_EMAIL instead."""
    log("SIMULATION – fake data, no TinyFish runs, the retailer is NOT emailed")
    state = load_state()
    item = next(iter(state["items"].values()), None) or SAMPLE_ITEM
    retailer = next((r for r in RETAILERS if r["name"] == item["retailer"]), RETAILERS[0])
    current = round(max(item["price_paid"] - drop, 0.01), 2)
    paid, actual_drop = item["price_paid"], round(item["price_paid"] - current, 2)

    stage("scan")
    log(f"Scanning orders: {retailer['name']} [simulated]")
    log(f"  ✓ logged in as {MY_NAME}")
    log(f"  order {item['order_id']} placed {item['order_date']} | 1 item(s)")
    log(f"    - {short(item['name'])}: paid £{paid:.2f}")
    time.sleep(1)

    stage("check")
    log("Checking 1 item price(s) [simulated]")
    log(f"  starting check: {short(item['name'], 45)}")
    log(f"    {item['product_url']}")
    time.sleep(1.5)
    log(f"  ✓ {short(item['name'], 45)}: found £{current:.2f} (in stock)")

    stage("compare")
    log("Comparing prices")
    log(f"  {short(item['name'], 45)}: paid £{paid:.2f}, now £{current:.2f} – ALERT: £{actual_drop:.2f} drop")
    time.sleep(1)

    stage("notify")
    claim = claim_text(item, current)
    log(f"  sending mock claim to {MY_EMAIL} (would normally go to {retailer['claim_email']})…")
    send_email(
        MY_EMAIL,
        f"[SIMULATION] Price adjustment request – order {item['order_id']}",
        f"*** SIMULATION ***\nThis is the claim the tracker would send to {retailer['claim_email']} "
        f"when {item['name']} drops from £{paid:.2f} to £{current:.2f}.\n\n----- claim -----\n{claim}",
    )
    log(f"  ✓ mock claim emailed to {MY_EMAIL}")
    emit({"type": "alert", "name": item["name"], "retailer": item["retailer"], "paid": paid,
          "now": current, "drop": actual_drop, "to": MY_EMAIL, "sent": True, "simulated": True})
    stage("done")
    log("Simulation finished.")


if __name__ == "__main__":
    if "--simulate" in sys.argv:
        simulate_price_drop()
    else:
        main()