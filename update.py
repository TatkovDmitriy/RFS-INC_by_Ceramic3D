#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ceramic 3D — ежедневное обновление обращений в Google Sheets + HTML-дашборд
Запускается через GitHub Actions (05:00 UTC = 08:00 МСК)
"""
import os, requests, warnings, sys, re, json, urllib.request, urllib.parse, urllib3, time
from datetime import datetime, timezone, timedelta
from collections import Counter, defaultdict
from exchangelib import Credentials, Account, DELEGATE, Configuration, BaseProtocol

warnings.filterwarnings('ignore')
urllib3.disable_warnings()

# ── Конфигурация ─────────────────────────────────────────────────────────────
SHEET_ID         = '1afKOZkU1YdLM5JXOzXGq36K5NR4ZyMiUHhAB8JdGZK4'
KEYS_SHEET_ID    = '1vf7W7oXXBEFwW37QV1CMy2sH2zkHUzrAHbmFj9P2-YE'
APPS_SCRIPT_URL  = os.environ.get('APPS_SCRIPT_URL', '')
KEYS_SHEETS      = ['Кухни_Лицензии', 'Ванные_Лицензии', 'Кухни_Рендер', 'Ванные_Рендер']
TOKEN_PATH       = os.environ.get('TOKEN_PATH', 'C:/Users/tatko/.config/google-docs-mcp/token.json')
GOOGLE_CLIENT_ID = os.environ.get('GOOGLE_CLIENT_ID', '398606258856-mvvu5it2p7hgro3s3s6msfbjf6f7uf70.apps.googleusercontent.com')
CLIENT_SECRET    = os.environ.get('GOOGLE_CLIENT_SECRET', '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')
GOOGLE_REFRESH_TOKEN = os.environ.get('GOOGLE_REFRESH_TOKEN', '')
EWS_SERVER       = 'owa.lemanapro.ru'
EWS_USER         = '60110579'
EWS_PASS         = os.environ.get('EWS_PASS', 'HQob6bn1xktt6yhnmju7')
DATE_FROM        = datetime(2026, 4, 1, tzinfo=timezone.utc)
MSK              = timezone(timedelta(hours=3))
HTML_OUT         = os.environ.get('HTML_OUT', 'index.html')
LOG_FILE         = os.environ.get('LOG_FILE', 'update.log')

# Цвета для Google Sheets
NAVY         = {'red': 0.075, 'green': 0.22,  'blue': 0.45}
NAVY_LIGHT   = {'red': 0.16,  'green': 0.33,  'blue': 0.55}
TEAL         = {'red': 0.035, 'green': 0.38,  'blue': 0.28}
TEAL_LIGHT   = {'red': 0.14,  'green': 0.44,  'blue': 0.35}
WHITE        = {'red': 1.0,   'green': 1.0,   'blue': 1.0}
GRAY_LIGHT   = {'red': 0.97,  'green': 0.97,  'blue': 0.97}
BLUE_PALE    = {'red': 0.93,  'green': 0.96,  'blue': 1.0}
TEAL_PALE    = {'red': 0.91,  'green': 0.97,  'blue': 0.94}
RED_PALE     = {'red': 1.0,   'green': 0.91,  'blue': 0.91}
AMBER_PALE   = {'red': 1.0,   'green': 0.96,  'blue': 0.86}
GREEN_PALE   = {'red': 0.91,  'green': 1.0,   'blue': 0.93}
GOLD         = {'red': 1.0,   'green': 0.84,  'blue': 0.0}

def log(msg):
    ts   = datetime.now(MSK).strftime('%d.%m.%Y %H:%M:%S')
    line = f'[{ts}] {msg}'
    print(line)
    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(line + '\n')
    except:
        pass

# ── EWS ──────────────────────────────────────────────────────────────────────
class NoSSL(requests.adapters.HTTPAdapter):
    def send(self, *a, **kw):
        kw['verify'] = False
        return super().send(*a, **kw)

BaseProtocol.HTTP_ADAPTER_CLS = NoSSL

def connect_ews():
    creds  = Credentials(EWS_USER, EWS_PASS)
    config = Configuration(server=EWS_SERVER, credentials=creds)
    return Account(f'{EWS_USER}@lemanapro.ru', config=config, autodiscover=False, access_type=DELEGATE)

# ── HTML / Parsing ────────────────────────────────────────────────────────────
def strip_html(html):
    h = re.sub(r'<style[^>]*>.*?</style>', '', html or '', flags=re.DOTALL)
    h = re.sub(r'<[^>]+>', ' ', h)
    for ent, ch in [('&nbsp;',' '),('&#43;','+'),('&quot;','"'),('&amp;','&'),('&#40;','('),('&#41;',')')]:
        h = h.replace(ent, ch)
    return re.sub(r'\s+', ' ', h).strip()

def find(pattern, text, default=''):
    m = re.search(pattern, text, re.IGNORECASE)
    return m.group(1).strip() if m else default

def parse_email(msg):
    subj = msg.subject or ''
    tm   = re.search(r'((?:INC|RFS|CHG)\d+_\d+)', subj)
    num  = tm.group(1) if tm else ''
    typ  = num[:3] if num else 'OTHER'
    text = strip_html(msg.body)

    cat      = find(r'Ceramic 3D / ([^\n]+?)(?:\s{2,}|Описание|$)', text)
    desc     = find(r'Описание:\s*(.+?)(?:\s{2,}|Дополнительная|Приоритет|$)', text)
    add_info = find(r'Дополнительная информация:\s*(.+?)(?:\s{2,}|Приоритет|Статус|$)', text)[:300]
    priority = find(r'Приоритет:\s*(\S[^\n]{0,20})', text)
    reg_date = find(r'Дата регистрации\s*:\s*([\d\./ :]+)', text)
    deadline = find(r'не позднее\s*:\s*([\d\./ :]+)', text)
    contact  = find(r'Контакт:\s*([^(\n]+?)(?:\s*\(|\s{2,})', text)
    emp_id   = find(r'Контакт:[^(]+\((\d+)\)', text)
    store    = find(r'Магазин:\s*(.+?)(?:\s{2,}|Открыть|$)', text)

    dt = msg.datetime_received
    if dt:
        try:
            std_dt = datetime.fromtimestamp(dt.timestamp(), tz=timezone.utc).astimezone(MSK)
            dt_str = std_dt.strftime('%d.%m.%Y %H:%M')
            dt     = std_dt
        except Exception:
            dt_str = str(dt)[:16]
    else:
        dt_str = ''

    return dict(
        ticket=num, type=typ, subject=subj,
        received_str=dt_str, received_dt=dt,
        reg_date=reg_date.strip(), deadline=deadline.strip(),
        priority=priority.strip(), category=cat.strip(),
        description=desc.strip(), add_info=add_info.strip(),
        contact=contact.strip(), emp_id=emp_id.strip(), store=store.strip()
    )

# ── Keys spreadsheet ─────────────────────────────────────────────────────────
def sheets_read(tok, sid, range_):
    url = f'https://sheets.googleapis.com/v4/spreadsheets/{sid}/values/{urllib.parse.quote(range_)}'
    hdr = {'Authorization': f'Bearer {tok}'}
    req = urllib.request.Request(url, headers=hdr)
    try:
        return json.loads(urllib.request.urlopen(req).read()).get('values', [])
    except Exception as ex:
        log(f'sheets_read {range_}: {ex}')
        return []

def fetch_keys(tok):
    result = {}
    for sheet in KEYS_SHEETS:
        rows = sheets_read(tok, KEYS_SHEET_ID, f'{sheet}!A:Z')
        if not rows:
            result[sheet] = []
            continue
        headers = [h.strip() for h in rows[0]]
        status_idx = next((i for i, h in enumerate(headers) if h.strip() == 'Статус'), None)
        available = []
        for row in rows[1:]:
            row_p = row + [''] * max(0, len(headers) - len(row))
            status = row_p[status_idx].strip() if status_idx is not None else ''
            if not status:
                available.append(dict(zip(headers, row_p)))
        result[sheet] = available
        log(f'  Ключи {sheet}: {len(available)} доступно')
    return result

# ── Google Sheets API ─────────────────────────────────────────────────────────
def get_token():
    rt = GOOGLE_REFRESH_TOKEN
    if not rt:
        with open(TOKEN_PATH) as f:
            tok_data = json.load(f)
        rt = tok_data['refresh_token']
    d = urllib.parse.urlencode({
        'client_id': GOOGLE_CLIENT_ID, 'client_secret': CLIENT_SECRET,
        'refresh_token': rt, 'grant_type': 'refresh_token'
    }).encode()
    r = urllib.request.urlopen(urllib.request.Request(
        'https://oauth2.googleapis.com/token', data=d, method='POST'))
    return json.loads(r.read())['access_token']

def api(tok, method, path, body=None):
    url = f'https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}{path}'
    hdr = {'Authorization': f'Bearer {tok}', 'Content-Type': 'application/json; charset=utf-8'}
    dat = json.dumps(body, ensure_ascii=False).encode('utf-8') if body else None
    req = urllib.request.Request(url, data=dat, headers=hdr, method=method)
    try:
        return json.loads(urllib.request.urlopen(req).read())
    except urllib.error.HTTPError as e:
        err = e.read().decode()
        log(f'API {e.code}: {err[:200]}')
        return None

def clear_and_write(tok, sheet_name, rows, cols='A:M'):
    api(tok, 'POST', f'/values/{urllib.parse.quote(sheet_name)}!{cols}:clear', {})
    BATCH = 400
    for i in range(0, len(rows), BATCH):
        batch  = rows[i:i+BATCH]
        sr, er = i+1, i+len(batch)
        clean  = [[''.join(c for c in str(v) if ord(c)>=32)[:400] for v in row] for row in batch]
        rng    = urllib.parse.quote(f'{sheet_name}!A{sr}:M{er}')
        api(tok, 'PUT', f'/values/{rng}?valueInputOption=RAW', {'values': clean})

def batch_update(tok, reqs):
    if reqs:
        api(tok, 'POST', ':batchUpdate', {'requests': reqs})

def ensure_rows(tok, sheet_gid, n):
    batch_update(tok, [{'updateSheetProperties': {
        'properties': {'sheetId': sheet_gid, 'gridProperties': {'rowCount': n}},
        'fields': 'gridProperties.rowCount'
    }}])

# ── Sheet management ──────────────────────────────────────────────────────────
SHEET_NAMES = ['INC — Инциденты', 'RFS — Заявки', 'Дашборд']

def setup_sheets(tok):
    info     = api(tok, 'GET', '')
    existing = {s['properties']['title']: s['properties'] for s in info['sheets']}
    reqs     = []
    for name in SHEET_NAMES:
        if name not in existing:
            reqs.append({'addSheet': {'properties': {'title': name}}})
    for old in ['Обращения', 'Sheet1']:
        if old in existing:
            reqs.append({'deleteSheet': {'sheetId': existing[old]['sheetId']}})
    if reqs:
        batch_update(tok, reqs)
    info = api(tok, 'GET', '')
    return {s['properties']['title']: s['properties']['sheetId'] for s in info['sheets']}

# ── Data rows ─────────────────────────────────────────────────────────────────
HEADERS = ['№ Обращения', 'Получено (МСК)', 'Дата регистрации', 'Срок решения',
           'Приоритет', 'Категория', 'Описание', 'Доп. информация',
           'Контакт', 'Таб. №', 'Магазин']

def to_row(p):
    return [p['ticket'], p['received_str'], p['reg_date'], p['deadline'],
            p['priority'], p['category'], p['description'], p['add_info'][:200],
            p['contact'], p['emp_id'], p['store']]

# ── Formatting ────────────────────────────────────────────────────────────────
def color(r,g,b): return {'red':r,'green':g,'blue':b}
def txt(bold=False, size=9, fg=None, italic=False):
    t = {'bold': bold, 'fontSize': size}
    if fg: t['foregroundColor'] = fg
    if italic: t['italic'] = italic
    return t
def cell_fmt(bg=None, text=None, align='LEFT', valign='MIDDLE', wrap='CLIP'):
    f = {'horizontalAlignment': align, 'verticalAlignment': valign, 'wrapStrategy': wrap}
    if bg: f['backgroundColor'] = bg
    if text: f['textFormat'] = text
    return f

def fmt_range(sid, r1, r2, c1=0, c2=11):
    return {'sheetId': sid, 'startRowIndex': r1, 'endRowIndex': r2,
            'startColumnIndex': c1, 'endColumnIndex': c2}

def repeat(sid, r1, r2, c1, c2, fmt):
    return {'repeatCell': {'range': fmt_range(sid,r1,r2,c1,c2),
            'cell': {'userEnteredFormat': fmt},
            'fields': 'userEnteredFormat(' + ','.join(fmt.keys()) + ')'}}

def remove_bandings(tok, sid):
    info = api(tok, 'GET', '')
    for s in info.get('sheets', []):
        if s['properties']['sheetId'] == sid:
            for br in s.get('bandedRanges', []):
                batch_update(tok, [{'deleteBanding': {'bandedRangeId': br['bandedRangeId']}}])

def format_data_sheet(tok, sid, rows, is_inc):
    remove_bandings(tok, sid)
    hdr_color  = NAVY if is_inc else TEAL
    pale_color = BLUE_PALE if is_inc else TEAL_PALE
    n = len(rows) + 1

    reqs = [
        repeat(sid, 0, 1, 0, 11, cell_fmt(bg=hdr_color, text=txt(True,10,WHITE), align='CENTER', wrap='WRAP')),
        {'updateDimensionProperties': {'range': {'sheetId':sid,'dimension':'ROWS','startIndex':0,'endIndex':1},
            'properties': {'pixelSize': 38}, 'fields': 'pixelSize'}},
        {'updateSheetProperties': {'properties': {'sheetId':sid,'gridProperties':{'frozenRowCount':1}},
            'fields': 'gridProperties.frozenRowCount'}},
        {'addBanding': {'bandedRange': {
            'range': fmt_range(sid, 1, max(n,2)),
            'rowProperties': {'firstBandColor': WHITE, 'secondBandColor': pale_color}
        }}},
        repeat(sid, 1, n, 0, 11, cell_fmt(text=txt(False,9), wrap='CLIP')),
        *[{'updateDimensionProperties': {'range': {'sheetId':sid,'dimension':'COLUMNS','startIndex':i,'endIndex':i+1},
            'properties': {'pixelSize': w}, 'fields': 'pixelSize'}}
          for i, w in enumerate([150,120,120,120,85,190,240,220,130,75,185])],
    ]

    prio_map = {'критич': RED_PALE, 'высок': RED_PALE, 'средн': AMBER_PALE, 'низк': GREEN_PALE}
    for i, row in enumerate(rows):
        p = row['priority'].lower()
        for k, col in prio_map.items():
            if k in p:
                reqs.append(repeat(sid, i+1, i+2, 4, 5, {'backgroundColor': col}))
                break

    reqs.append({'updateBorders': {'range': fmt_range(sid, 0, 1),
        'bottom': {'style':'SOLID_MEDIUM','color':{'red':1,'green':1,'blue':1,'alpha':0.3}}}})

    batch_update(tok, reqs)

# ── Dashboard ─────────────────────────────────────────────────────────────────
def write_dashboard(tok, sid, inc_rows, rfs_rows):
    now       = datetime.now(MSK)
    week_ago  = now - timedelta(days=7)
    month_ago = now - timedelta(days=30)

    def cnt(rows, since=None):
        if since is None: return len(rows)
        return sum(1 for r in rows if r['received_dt'] and r['received_dt'] >= since)

    weekly = []
    for w in range(4, -1, -1):
        ws = (now - timedelta(days=now.weekday() + 7*w)).replace(hour=0,minute=0,second=0,microsecond=0)
        we = ws + timedelta(days=7)
        label = f'{ws.strftime("%d.%m")}–{(we-timedelta(1)).strftime("%d.%m")}'
        ic = sum(1 for r in inc_rows if r['received_dt'] and ws <= r['received_dt'] < we)
        rc = sum(1 for r in rfs_rows if r['received_dt'] and ws <= r['received_dt'] < we)
        weekly.append([label, ic, rc, ic+rc])

    daily = []
    for d in range(13, -1, -1):
        ds = (now - timedelta(days=d)).replace(hour=0,minute=0,second=0,microsecond=0)
        de = ds + timedelta(days=1)
        label = ds.strftime('%d.%m')
        ic = sum(1 for r in inc_rows if r['received_dt'] and ds <= r['received_dt'] < de)
        rc = sum(1 for r in rfs_rows if r['received_dt'] and ds <= r['received_dt'] < de)
        daily.append([label, ic, rc, ic+rc])

    inc_cats  = Counter(r['category'] for r in inc_rows if r['category']).most_common(10)
    rfs_cats  = Counter(r['category'] for r in rfs_rows if r['category']).most_common(10)
    inc_prios = Counter(r['priority'] for r in inc_rows if r['priority']).most_common()

    upd = now.strftime('%d.%m.%Y в %H:%M МСК')
    R   = []
    def row(*vals): R.append(list(vals) + ['']*(6-len(vals)))
    def blank(): R.append(['']*6)

    row(f'Дашборд — L3-C3D-Ceramic3D')
    row(f'Обновлено: {upd}')
    blank()
    row('Общая статистика')
    row('', 'INC (Инциденты)', 'RFS (Заявки)', 'Итого')
    row('Всего с 01.04.2026',       cnt(inc_rows),          cnt(rfs_rows),          cnt(inc_rows)+cnt(rfs_rows))
    row('За 7 дней',                cnt(inc_rows,week_ago),  cnt(rfs_rows,week_ago),  cnt(inc_rows,week_ago)+cnt(rfs_rows,week_ago))
    row('За 30 дней',               cnt(inc_rows,month_ago), cnt(rfs_rows,month_ago), cnt(inc_rows,month_ago)+cnt(rfs_rows,month_ago))
    blank()
    row('По неделям')
    row('Неделя', 'INC', 'RFS', 'Итого')
    WEEKLY_START = len(R)
    for w in weekly: row(*w)
    blank()
    row('По дням (последние 14 дней)')
    row('Дата', 'INC', 'RFS', 'Итого')
    DAILY_START = len(R)
    for d in daily: row(*d)
    blank()
    row('Топ категорий INC')
    row('Категория', 'Кол-во')
    for cat, n in inc_cats: row(cat, n)
    blank()
    row('Топ категорий RFS')
    row('Категория', 'Кол-во')
    for cat, n in rfs_cats: row(cat, n)
    blank()
    row('Приоритеты INC')
    row('Приоритет', 'Кол-во')
    for p, n in inc_prios: row(p, n)

    ensure_rows(tok, sid, max(len(R) + 20, 200))
    api(tok, 'POST', f'/values/{urllib.parse.quote("Дашборд")}!A:F:clear', {})
    clean = [[''.join(c for c in str(v) if ord(c)>=32)[:300] for v in r] for r in R]
    rng   = urllib.parse.quote(f'Дашборд!A1:F{len(R)}')
    api(tok, 'PUT', f'/values/{rng}?valueInputOption=RAW', {'values': clean})

    format_dashboard(tok, sid, len(R), WEEKLY_START, len(weekly), DAILY_START, len(daily))
    add_charts(tok, sid, WEEKLY_START, len(weekly), DAILY_START, len(daily))
    log(f'  Дашборд: {len(R)} строк')

def format_dashboard(tok, sid, n_rows, ws, nw, ds, nd):
    remove_bandings(tok, sid)
    reqs = [
        {'updateDimensionProperties': {'range': {'sheetId':sid,'dimension':'COLUMNS','startIndex':0,'endIndex':1}, 'properties':{'pixelSize':210},'fields':'pixelSize'}},
        {'updateDimensionProperties': {'range': {'sheetId':sid,'dimension':'COLUMNS','startIndex':1,'endIndex':5}, 'properties':{'pixelSize':110},'fields':'pixelSize'}},
        {'mergeCells': {'range': fmt_range(sid,0,1,0,6), 'mergeType':'MERGE_ALL'}},
        repeat(sid, 0, 1, 0, 6, {'backgroundColor':NAVY,'textFormat':txt(True,17,WHITE),'horizontalAlignment':'CENTER','verticalAlignment':'MIDDLE'}),
        {'updateDimensionProperties': {'range':{'sheetId':sid,'dimension':'ROWS','startIndex':0,'endIndex':1},'properties':{'pixelSize':54},'fields':'pixelSize'}},
        {'mergeCells': {'range': fmt_range(sid,1,2,0,6), 'mergeType':'MERGE_ALL'}},
        repeat(sid, 1, 2, 0, 6, {'backgroundColor':NAVY_LIGHT,'textFormat':txt(False,10,color(0.75,0.87,0.98),True),'horizontalAlignment':'CENTER'}),
        {'updateSheetProperties': {'properties':{'sheetId':sid,'gridProperties':{'frozenRowCount':2}},'fields':'gridProperties.frozenRowCount'}},
    ]

    def add_section(row_idx):
        reqs.append({'mergeCells': {'range': fmt_range(sid,row_idx,row_idx+1,0,6),'mergeType':'MERGE_ALL'}})
        reqs.append(repeat(sid,row_idx,row_idx+1,0,6,{'backgroundColor':color(0.88,0.92,0.97),'textFormat':txt(True,11,NAVY),'verticalAlignment':'MIDDLE'}))
        reqs.append({'updateDimensionProperties':{'range':{'sheetId':sid,'dimension':'ROWS','startIndex':row_idx,'endIndex':row_idx+1},'properties':{'pixelSize':32},'fields':'pixelSize'}})

    for si in [3, 9, ws-2, ds-2]:
        if 0 <= si < n_rows:
            add_section(si)

    reqs.append(repeat(sid,4,5,0,4,{'backgroundColor':NAVY,'textFormat':txt(True,9,WHITE),'horizontalAlignment':'CENTER'}))
    reqs.append(repeat(sid,5,8,1,4,{'textFormat':txt(True,13),'horizontalAlignment':'CENTER','backgroundColor':color(0.97,0.98,1.0)}))
    reqs.append(repeat(sid,5,8,0,1,{'textFormat':txt(False,10),'verticalAlignment':'MIDDLE'}))

    for hi in [ws-1, ds-1]:
        if 0 <= hi < n_rows:
            reqs.append(repeat(sid,hi,hi+1,0,4,{'backgroundColor':color(0.22,0.38,0.56),'textFormat':txt(True,9,WHITE),'horizontalAlignment':'CENTER'}))

    if nw > 0:
        reqs.append({'addBanding': {'bandedRange': {'range': fmt_range(sid,ws,ws+nw,0,4),
            'rowProperties': {'firstBandColor': WHITE, 'secondBandColor': BLUE_PALE}}}})
    if nd > 0:
        reqs.append({'addBanding': {'bandedRange': {'range': fmt_range(sid,ds,ds+nd,0,4),
            'rowProperties': {'firstBandColor': WHITE, 'secondBandColor': TEAL_PALE}}}})

    batch_update(tok, reqs)

def add_charts(tok, sid, ws, nw, ds, nd):
    charts = []

    def src(r1, r2, c1, c2):
        return {'sourceRange': {'sources': [{'sheetId':sid,'startRowIndex':r1,'endRowIndex':r2,'startColumnIndex':c1,'endColumnIndex':c2}]}}

    if nw > 0:
        charts.append({'addChart': {'chart': {
            'spec': {
                'title': 'Обращения по неделям (INC vs RFS)',
                'titleTextFormat': {'bold': True, 'fontSize': 11},
                'basicChart': {
                    'chartType': 'COLUMN',
                    'legendPosition': 'BOTTOM_LEGEND',
                    'axis': [{'position': 'BOTTOM_AXIS'}, {'position': 'LEFT_AXIS', 'title': 'Количество'}],
                    'domains': [{'domain': src(ws, ws+nw, 0, 1)}],
                    'series': [
                        {'series': src(ws, ws+nw, 1, 2), 'targetAxis': 'LEFT_AXIS', 'color': {'red':0.075,'green':0.22,'blue':0.45}},
                        {'series': src(ws, ws+nw, 2, 3), 'targetAxis': 'LEFT_AXIS', 'color': {'red':0.035,'green':0.38,'blue':0.28}},
                    ],
                    'headerCount': 0
                }
            },
            'position': {'overlayPosition': {
                'anchorCell': {'sheetId':sid,'rowIndex':3,'columnIndex':4},
                'widthPixels': 480, 'heightPixels': 260
            }}
        }}})

    if nd > 0:
        charts.append({'addChart': {'chart': {
            'spec': {
                'title': 'Динамика по дням (14 дней)',
                'titleTextFormat': {'bold': True, 'fontSize': 11},
                'basicChart': {
                    'chartType': 'LINE',
                    'legendPosition': 'BOTTOM_LEGEND',
                    'axis': [{'position': 'BOTTOM_AXIS'}, {'position': 'LEFT_AXIS', 'title': 'Количество'}],
                    'domains': [{'domain': src(ds, ds+nd, 0, 1)}],
                    'series': [
                        {'series': src(ds, ds+nd, 1, 2), 'targetAxis': 'LEFT_AXIS', 'color': {'red':0.075,'green':0.22,'blue':0.45}},
                        {'series': src(ds, ds+nd, 2, 3), 'targetAxis': 'LEFT_AXIS', 'color': {'red':0.035,'green':0.38,'blue':0.28}},
                        {'series': src(ds, ds+nd, 3, 4), 'targetAxis': 'LEFT_AXIS', 'color': {'red':0.85,'green':0.55,'blue':0.0}},
                    ],
                    'headerCount': 0
                }
            },
            'position': {'overlayPosition': {
                'anchorCell': {'sheetId':sid,'rowIndex':17,'columnIndex':4},
                'widthPixels': 480, 'heightPixels': 260
            }}
        }}})

    if charts:
        batch_update(tok, charts)

# ── HTML Dashboard Generator ──────────────────────────────────────────────────
def generate_html(inc_rows, rfs_rows, updated_at, keys_data=None):
    now       = updated_at
    week_ago  = now - timedelta(days=7)
    month_ago = now - timedelta(days=30)

    def cnt(rows, since=None):
        if since is None: return len(rows)
        return sum(1 for r in rows if r['received_dt'] and r['received_dt'] >= since)

    weekly = []
    for w in range(4, -1, -1):
        ws = (now - timedelta(days=now.weekday() + 7*w)).replace(hour=0,minute=0,second=0,microsecond=0)
        we = ws + timedelta(days=7)
        label = f'{ws.strftime("%d.%m")}–{(we-timedelta(1)).strftime("%d.%m")}'
        ic = sum(1 for r in inc_rows if r['received_dt'] and ws <= r['received_dt'] < we)
        rc = sum(1 for r in rfs_rows if r['received_dt'] and ws <= r['received_dt'] < we)
        weekly.append({'label': label, 'inc': ic, 'rfs': rc, 'total': ic+rc})

    daily = []
    for d in range(13, -1, -1):
        ds = (now - timedelta(days=d)).replace(hour=0,minute=0,second=0,microsecond=0)
        de = ds + timedelta(days=1)
        label = ds.strftime('%d.%m')
        ic = sum(1 for r in inc_rows if r['received_dt'] and ds <= r['received_dt'] < de)
        rc = sum(1 for r in rfs_rows if r['received_dt'] and ds <= r['received_dt'] < de)
        daily.append({'label': label, 'inc': ic, 'rfs': rc, 'total': ic+rc})

    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    def ser(rows):
        return [{'ticket': r['ticket'], 'received': r['received_str'],
                 'reg_date': r['reg_date'], 'deadline': r['deadline'],
                 'priority': r['priority'], 'category': r['category'],
                 'description': r['description'], 'add_info': r['add_info'],
                 'contact': r['contact'], 'emp_id': r['emp_id'], 'store': r['store']} for r in rows]

    data = {
        'updated': updated_at.strftime('%d.%m.%Y %H:%M МСК'),
        'inc': ser(inc_rows),
        'rfs': ser(rfs_rows),
        'weekly': weekly,
        'daily': daily,
        'keys': keys_data or {},
        'apps_script_url': APPS_SCRIPT_URL,
        'stats': {
            'inc_total': cnt(inc_rows),
            'rfs_total': cnt(rfs_rows),
            'inc_week':  cnt(inc_rows, week_ago),
            'rfs_week':  cnt(rfs_rows, week_ago),
            'inc_today': cnt(inc_rows, today_start),
            'rfs_today': cnt(rfs_rows, today_start),
        }
    }

    data_json = json.dumps(data, ensure_ascii=False)

    template = r"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Ceramic3D — L3 Обращения</title>
<link href="https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
:root{--bg:#0d1117;--sur:#161b22;--sur2:#21262d;--brd:#30363d;--txt:#e6edf3;--muted:#8b949e;--nvy:#2563eb;--tel:#059669;}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Manrope',sans-serif;background:var(--bg);color:var(--txt);min-height:100vh}
a{color:var(--nvy);text-decoration:none}a:hover{text-decoration:underline}
/* Header */
.hdr{background:linear-gradient(135deg,#0a1628 0%,#0d2040 100%);border-bottom:1px solid var(--brd);padding:18px 28px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100}
.hdr-l{display:flex;align-items:center;gap:14px}
.hdr-logo{width:42px;height:42px;background:linear-gradient(135deg,#2563eb,#059669);border-radius:11px;display:flex;align-items:center;justify-content:center;font-size:21px;flex-shrink:0}
.hdr-title{font-size:17px;font-weight:800;letter-spacing:-0.3px}
.hdr-sub{font-size:11px;color:var(--muted);margin-top:3px}
.hdr-r{text-align:right}
.upd-lbl{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px}
.upd-val{font-size:13px;font-weight:700}
/* Main */
.main{max-width:1560px;margin:0 auto;padding:22px 28px}
/* Stats */
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:20px}
.sc{background:var(--sur);border:1px solid var(--brd);border-radius:12px;padding:18px 20px;position:relative;overflow:hidden}
.sc::before{content:'';position:absolute;top:0;left:0;right:0;height:3px}
.sc.inc::before{background:#2563eb}.sc.rfs::before{background:#059669}.sc.wk::before{background:#8b5cf6}.sc.td::before{background:#f59e0b}
.sc-lbl{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px}
.sc-val{font-size:38px;font-weight:800;line-height:1}
.sc.inc .sc-val{color:#60a5fa}.sc.rfs .sc-val{color:#34d399}.sc.wk .sc-val{color:#a78bfa}.sc.td .sc-val{color:#fbbf24}
.sc-sub{font-size:11px;color:var(--muted);margin-top:5px}
/* Charts */
.charts{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:20px}
.cc{background:var(--sur);border:1px solid var(--brd);border-radius:12px;padding:18px 20px}
.cc-title{font-size:11px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:14px}
.cc-wrap{height:190px}
/* Table section */
.ts{background:var(--sur);border:1px solid var(--brd);border-radius:12px;overflow:hidden}
.toolbar{padding:14px 18px;display:flex;align-items:center;gap:10px;border-bottom:1px solid var(--brd);flex-wrap:wrap}
.tabs{display:flex;gap:4px}
.tb{padding:6px 14px;border-radius:7px;border:1px solid var(--brd);background:transparent;color:var(--muted);font-family:'Manrope',sans-serif;font-size:13px;font-weight:600;cursor:pointer;transition:all .15s}
.tb.act-inc{background:rgba(37,99,235,.18);border-color:#2563eb;color:#60a5fa}
.tb.act-rfs{background:rgba(5,150,105,.18);border-color:#059669;color:#34d399}
.tb-cnt{display:inline-flex;align-items:center;justify-content:center;min-width:20px;height:20px;border-radius:10px;font-size:11px;font-weight:700;margin-left:6px;background:var(--sur2);color:var(--muted)}
.tb.act-inc .tb-cnt{background:#2563eb;color:#fff}.tb.act-rfs .tb-cnt{background:#059669;color:#fff}
.srch{flex:1;min-width:200px;background:var(--sur2);border:1px solid var(--brd);border-radius:8px;padding:8px 12px;color:var(--txt);font-family:'Manrope',sans-serif;font-size:13px}
.srch::placeholder{color:var(--muted)}.srch:focus{outline:none;border-color:#2563eb}
select.flt{background:var(--sur2);border:1px solid var(--brd);border-radius:8px;padding:8px 10px;color:var(--txt);font-family:'Manrope',sans-serif;font-size:12px;cursor:pointer;max-width:160px}
select.flt:focus{outline:none;border-color:#2563eb}
.rcnt{font-size:12px;color:var(--muted);margin-left:auto;white-space:nowrap}
/* Table */
.twrap{overflow-x:auto}
table{width:100%;border-collapse:collapse}
thead th{background:var(--sur2);padding:11px 14px;text-align:left;font-size:10px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;white-space:nowrap;border-bottom:1px solid var(--brd);cursor:pointer;user-select:none}
thead th:hover{color:var(--txt)}thead th.srt{color:#60a5fa}
tbody tr{border-bottom:1px solid var(--brd);cursor:pointer;transition:background .1s}
tbody tr:hover{background:rgba(255,255,255,.03)}
tbody td{padding:9px 14px;font-size:13px;max-width:280px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.tn{font-family:'Courier New',monospace;font-size:12px;font-weight:700;color:#60a5fa}
.rfs-tbl .tn{color:#34d399}
.pb{display:inline-flex;align-items:center;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600}
.p-crit{background:rgba(239,68,68,.2);color:#f87171}
.p-high{background:rgba(249,115,22,.2);color:#fb923c}
.p-med{background:rgba(234,179,8,.2);color:#facc15}
.p-low{background:rgba(34,197,94,.2);color:#4ade80}
.p-oth{background:rgba(139,148,158,.2);color:var(--muted)}
/* Modal */
.ov{display:none;position:fixed;inset:0;background:rgba(0,0,0,.65);z-index:200;backdrop-filter:blur(5px);align-items:center;justify-content:center}
.ov.open{display:flex}
.dp{background:var(--sur);border:1px solid var(--brd);border-radius:16px;width:580px;max-width:92vw;max-height:88vh;overflow-y:auto;padding:26px;position:relative}
.dp-close{position:absolute;top:14px;right:14px;background:var(--sur2);border:1px solid var(--brd);border-radius:8px;color:var(--muted);font-size:20px;width:32px;height:32px;display:flex;align-items:center;justify-content:center;cursor:pointer;line-height:1}
.dp-close:hover{color:var(--txt)}
.dt{font-size:24px;font-weight:800;font-family:monospace;margin-bottom:5px}
.dt.inc{color:#60a5fa}.dt.rfs{color:#34d399}
.d-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin:14px 0}
.df label{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;display:block;margin-bottom:3px}
.df .v{font-size:13px;font-weight:500}
.dd{margin-top:14px}
.dd label{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;display:block;margin-bottom:6px}
.dd .v{font-size:13px;line-height:1.6;background:var(--sur2);padding:10px 12px;border-radius:8px;white-space:pre-wrap;word-break:break-word}
/* Savings section */
.sav-sec{background:var(--sur);border:1px solid var(--brd);border-radius:12px;padding:16px 20px;margin-bottom:20px}
.sav-hdr{display:flex;align-items:center;justify-content:space-between;margin-bottom:12px}
.sav-hdr-lbl{font-size:11px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.5px}
.sav-grid{display:grid;grid-template-columns:1fr 1fr 1.2fr 1.2fr 1.4fr;gap:12px}
.sav-card{background:var(--sur2);border:1px solid var(--brd);border-radius:8px;padding:12px 14px}
.sav-lbl{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.4px;margin-bottom:8px}
.sav-row{display:flex;justify-content:space-between;align-items:center;font-size:13px;padding:3px 0;border-bottom:1px solid rgba(255,255,255,.04)}
.sav-row:last-child{border-bottom:none}
.sav-row span{color:var(--muted)}
.sav-row strong{color:var(--txt);font-size:15px}
.sav-amt{font-size:22px;font-weight:800;color:#fbbf24;margin-top:6px;letter-spacing:-0.5px}
.sav-sub{font-size:11px;color:var(--muted);margin-top:3px}
.sav-total-card{background:linear-gradient(135deg,rgba(5,150,105,.12),rgba(37,99,235,.12));border-color:rgba(5,150,105,.4)}
.sav-grand{font-size:28px;font-weight:800;color:#34d399;margin-top:6px;letter-spacing:-0.5px}
@media(max-width:900px){.sav-grid{grid-template-columns:1fr 1fr}.sav-total-card{grid-column:1/-1}}
/* Status badges */
.st-badge{display:inline-flex;align-items:center;padding:2px 9px;border-radius:10px;font-size:11px;font-weight:700;white-space:nowrap}
.st-active{background:rgba(59,130,246,.2);color:#60a5fa}
.st-working{background:rgba(245,158,11,.2);color:#fbbf24}
.st-approval{background:rgba(139,92,246,.2);color:#a78bfa}
.st-done{background:rgba(34,197,94,.2);color:#4ade80}
.st-cancelled{background:rgba(248,113,113,.2);color:#f87171}
/* Workflow */
.wf-section{margin-top:20px;border-top:1px solid var(--brd);padding-top:18px}
.wf-header{display:flex;align-items:center;gap:10px;margin-bottom:14px}
.wf-lbl{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px}
.wf-action{margin-top:12px}
.wf-btn{padding:9px 18px;border-radius:8px;border:none;font-family:'Manrope',sans-serif;font-size:13px;font-weight:700;cursor:pointer;transition:opacity .15s}
.wf-btn:hover{opacity:.85}
.btn-take{background:#2563eb;color:#fff}
.btn-approval{background:#8b5cf6;color:#fff;margin-top:12px}
.btn-done{background:#059669;color:#fff}
.btn-reset{background:var(--sur2);color:var(--muted);border:1px solid var(--brd);font-size:12px;padding:6px 12px}
.btn-cancel-t{background:transparent;color:#f87171;border:1px solid rgba(248,113,113,.35);font-size:12px;padding:6px 12px}
.wf-ctrl{display:flex;gap:6px;margin-left:auto}
.wf-radio-group{display:flex;flex-direction:column;gap:8px;margin-bottom:12px}
.wf-radio{display:flex;align-items:center;gap:8px;font-size:13px;cursor:pointer}
.wf-radio input{cursor:pointer;accent-color:#8b5cf6}
.wf-selects{display:flex;flex-direction:column;gap:8px;margin-bottom:4px}
.wf-select{background:var(--sur2);border:1px solid var(--brd);border-radius:8px;padding:8px 10px;color:var(--txt);font-family:'Manrope',sans-serif;font-size:13px}
.wf-select:focus{outline:none;border-color:#8b5cf6}
.wf-info-box{background:var(--sur2);border:1px solid var(--brd);border-radius:8px;padding:12px;font-size:13px;margin-bottom:12px;line-height:1.6}
.wf-info-box strong{color:var(--txt)}
.wf-info-box .wf-meta{color:var(--muted);font-size:11px;margin-top:4px}
.cancel-form{margin-top:10px}
.cancel-input{width:100%;background:var(--sur2);border:1px solid rgba(248,113,113,.4);border-radius:8px;padding:10px;color:var(--txt);font-family:'Manrope',sans-serif;font-size:13px;resize:vertical;min-height:68px}
.cancel-input:focus{outline:none;border-color:#f87171}
.cancel-btns{display:flex;gap:8px;margin-top:8px}
/* Scrollbar */
::-webkit-scrollbar{width:6px;height:6px}
::-webkit-scrollbar-track{background:var(--sur)}
::-webkit-scrollbar-thumb{background:var(--brd);border-radius:3px}
::-webkit-scrollbar-thumb:hover{background:var(--muted)}
@media(max-width:900px){.main{padding:14px}.stats{grid-template-columns:repeat(2,1fr)}.charts{grid-template-columns:1fr}.hdr{padding:14px 16px}}
</style>
</head>
<body>
<script>const DATA=__DATA_JSON__;</script>

<header class="hdr">
  <div class="hdr-l">
    <div class="hdr-logo">📋</div>
    <div>
      <div class="hdr-title">Ceramic3D — L3 Обращения</div>
      <div class="hdr-sub">Инциденты и заявки · <a href="https://docs.google.com/spreadsheets/d/1afKOZkU1YdLM5JXOzXGq36K5NR4ZyMiUHhAB8JdGZK4" target="_blank">Google Sheets ↗</a></div>
    </div>
  </div>
  <div class="hdr-r">
    <div class="upd-lbl">Обновлено</div>
    <div class="upd-val" id="updtime"></div>
  </div>
</header>

<main class="main">
  <div class="stats">
    <div class="sc inc"><div class="sc-lbl">INC — Инциденты</div><div class="sc-val" id="s0">—</div><div class="sc-sub">с 01.04.2026</div></div>
    <div class="sc rfs"><div class="sc-lbl">RFS — Заявки</div><div class="sc-val" id="s1">—</div><div class="sc-sub">с 01.04.2026</div></div>
    <div class="sc wk"><div class="sc-lbl">За 7 дней</div><div class="sc-val" id="s2">—</div><div class="sc-sub" id="s2s"></div></div>
    <div class="sc td"><div class="sc-lbl">Сегодня</div><div class="sc-val" id="s3">—</div><div class="sc-sub" id="s3s"></div></div>
  </div>

  <div class="sav-sec">
    <div class="sav-hdr">
      <span class="sav-hdr-lbl">Экономия с 06.03.2026</span>
    </div>
    <div class="sav-grid">
      <div class="sav-card">
        <div class="sav-lbl">Ключей заменено</div>
        <div class="sav-row"><span>Кухни</span><strong id="sv-kk">—</strong></div>
        <div class="sav-row"><span>Ванные</span><strong id="sv-bk">—</strong></div>
        <div class="sav-row"><span>Итого</span><strong id="sv-tk">—</strong></div>
      </div>
      <div class="sav-card">
        <div class="sav-lbl">Рендеров заменено</div>
        <div class="sav-row"><span>Кухни</span><strong id="sv-kr">—</strong></div>
        <div class="sav-row"><span>Ванные</span><strong id="sv-br">—</strong></div>
        <div class="sav-row"><span>Итого</span><strong id="sv-tr">—</strong></div>
      </div>
      <div class="sav-card">
        <div class="sav-lbl">Экономия — ключи</div>
        <div class="sav-amt" id="sv-amt-k">—</div>
        <div class="sav-sub" id="sv-sub-k"></div>
      </div>
      <div class="sav-card">
        <div class="sav-lbl">Экономия — рендеры</div>
        <div class="sav-amt" id="sv-amt-r">—</div>
        <div class="sav-sub" id="sv-sub-r"></div>
      </div>
      <div class="sav-card sav-total-card">
        <div class="sav-lbl">Итого сэкономлено</div>
        <div class="sav-grand" id="sv-grand">—</div>
        <div class="sav-sub" id="sv-grand-sub"></div>
      </div>
    </div>
  </div>

  <div class="charts">
    <div class="cc"><div class="cc-title">По неделям (INC vs RFS)</div><div class="cc-wrap"><canvas id="cw"></canvas></div></div>
    <div class="cc"><div class="cc-title">Динамика · последние 14 дней</div><div class="cc-wrap"><canvas id="cd"></canvas></div></div>
  </div>

  <div class="ts">
    <div class="toolbar">
      <div class="tabs">
        <button class="tb act-inc" id="btn-inc" onclick="switchTab('inc')">INC<span class="tb-cnt" id="cn-inc">0</span></button>
        <button class="tb" id="btn-rfs" onclick="switchTab('rfs')">RFS<span class="tb-cnt" id="cn-rfs">0</span></button>
      </div>
      <input class="srch" type="text" id="srch" placeholder="🔍  Поиск по тикету, описанию, категории, магазину…" oninput="applyFilters()">
      <select class="flt" id="fp" onchange="applyFilters()"><option value="">Все приоритеты</option></select>
      <select class="flt" id="fc" onchange="applyFilters()"><option value="">Все категории</option></select>
      <span class="rcnt" id="rcnt"></span>
    </div>
    <div class="twrap">
      <table id="tbl">
        <thead><tr id="thead-row"></tr></thead>
        <tbody id="tbody"></tbody>
      </table>
    </div>
  </div>
</main>

<div class="ov" id="ov" onclick="closeD(event)">
  <div class="dp" id="dp">
    <div class="dp-close" onclick="closeD()">×</div>
    <div id="dc"></div>
    <div class="wf-section" id="workflow" style="display:none">
      <div class="wf-header">
        <div><span class="wf-lbl">Статус заявки</span> <span id="wf-status"></span></div>
        <div class="wf-ctrl" id="wf-ctrl" style="display:none">
          <button class="wf-btn btn-reset" onclick="resetTicket()">↺ Сбросить</button>
          <button class="wf-btn btn-cancel-t" id="wf-cancel-btn" onclick="showCancelForm()">✕ Отменить</button>
        </div>
      </div>
      <div id="wf-cancel-form" class="cancel-form" style="display:none">
        <textarea class="cancel-input" id="cancel-comment" placeholder="Укажите причину отмены..."></textarea>
        <div class="cancel-btns">
          <button class="wf-btn btn-cancel-t" onclick="cancelTicket()">Подтвердить отмену</button>
          <button class="wf-btn btn-reset" onclick="hideCancelForm()">← Назад</button>
        </div>
      </div>
      <div id="wf-take" class="wf-action" style="display:none">
        <button class="wf-btn btn-take" onclick="takeTicket()">▶ Взять в работу</button>
      </div>
      <div id="wf-resolve" class="wf-action" style="display:none">
        <div class="wf-radio-group">
          <label class="wf-radio"><input type="radio" name="rtype" value="new" onchange="toggleKeySel()"> Новый ключ</label>
          <label class="wf-radio"><input type="radio" name="rtype" value="replace" onchange="toggleKeySel()"> Замена старого ключа</label>
        </div>
        <div id="key-sel" class="wf-selects" style="display:none">
          <select id="sel-type" class="wf-select" onchange="updateKeyList()">
            <option value="">Тип...</option>
            <option value="Лицензии">Лицензия</option>
            <option value="Рендер">Рендер</option>
          </select>
          <select id="sel-cat" class="wf-select" onchange="updateKeyList()">
            <option value="">Категория...</option>
            <option value="Кухни">Кухни</option>
            <option value="Ванные">Ванные</option>
          </select>
          <select id="sel-key" class="wf-select"><option value="">Выберите ключ...</option></select>
        </div>
        <button class="wf-btn btn-approval" onclick="sendApproval()">→ Отправить на согласование</button>
      </div>
      <div id="wf-approval" class="wf-action" style="display:none">
        <div class="wf-info-box" id="wf-aprv-info"></div>
        <button class="wf-btn btn-done" onclick="completeTicket()">✓ Выполнено</button>
      </div>
      <div id="wf-done" class="wf-action" style="display:none">
        <div class="wf-info-box" id="wf-done-info"></div>
      </div>
      <div id="wf-cancelled" class="wf-action" style="display:none">
        <div class="wf-info-box" id="wf-cancelled-info"></div>
        <button class="wf-btn btn-reset" onclick="resetTicket()" style="margin-top:4px">↺ Восстановить заявку</button>
      </div>
    </div>
  </div>
</div>

<script>
// ── State ──────────────────────────────────────────────────────────────
let tab='inc',col='received',dir=-1,fd=[],curTicket=null;
const ST_KEY='c3d_rfs_statuses';
function getStatuses(){return JSON.parse(localStorage.getItem(ST_KEY)||'{}')}
function saveStatuses(s){localStorage.setItem(ST_KEY,JSON.stringify(s))}
function getTS(ticket){return getStatuses()[ticket]||{status:'Активна'}}
function setTS(ticket,data){const s=getStatuses();s[ticket]={...(s[ticket]||{}),...data};saveStatuses(s)}

// ── Status badge ───────────────────────────────────────────────────────
function stBadge(st){
  const m={'Активна':'st-active','В работе':'st-working','Согласование':'st-approval','Выполнено':'st-done','Отменена':'st-cancelled'};
  return`<span class="st-badge ${m[st]||'st-active'}">${st||'Активна'}</span>`;
}

// ── Init ───────────────────────────────────────────────────────────────
function init(){
  document.getElementById('updtime').textContent=DATA.updated;
  const s=DATA.stats;
  document.getElementById('s0').textContent=s.inc_total;
  document.getElementById('s1').textContent=s.rfs_total;
  document.getElementById('s2').textContent=s.inc_week+s.rfs_week;
  document.getElementById('s2s').textContent='INC: '+s.inc_week+' · RFS: '+s.rfs_week;
  document.getElementById('s3').textContent=s.inc_today+s.rfs_today;
  document.getElementById('s3s').textContent='INC: '+s.inc_today+' · RFS: '+s.rfs_today;
  document.getElementById('cn-inc').textContent=DATA.inc.length;
  document.getElementById('cn-rfs').textContent=DATA.rfs.length;
  buildFilters();renderCharts();applyFilters();calcSavings();
}
function buildFilters(){
  const all=[...DATA.inc,...DATA.rfs];
  const ps=[...new Set(all.map(r=>r.priority).filter(Boolean))].sort();
  const fp=document.getElementById('fp');
  ps.forEach(p=>{const o=document.createElement('option');o.value=p;o.textContent=p;fp.appendChild(o);});
  const cs=[...new Set(all.map(r=>r.category).filter(Boolean))].sort();
  const fc=document.getElementById('fc');
  cs.forEach(c=>{const o=document.createElement('option');o.value=c;o.textContent=c.length>45?c.slice(0,45)+'…':c;fc.appendChild(o);});
}
function switchTab(t){
  tab=t;
  ['inc','rfs'].forEach(x=>{
    const b=document.getElementById('btn-'+x);
    b.className='tb'+(x===t?' act-'+x:'');
  });
  document.getElementById('tbl').className=t+'-tbl';
  applyFilters();
}
function applyFilters(){
  const data=tab==='inc'?DATA.inc:DATA.rfs;
  const q=document.getElementById('srch').value.toLowerCase();
  const fp=document.getElementById('fp').value;
  const fc=document.getElementById('fc').value;
  fd=data.filter(r=>{
    if(q&&!['ticket','description','category','store','contact','add_info'].some(f=>(r[f]||'').toLowerCase().includes(q)))return false;
    if(fp&&r.priority!==fp)return false;
    if(fc&&r.category!==fc)return false;
    return true;
  });
  fd.sort((a,b)=>{const av=a[col]||'',bv=b[col]||'';return av<bv?dir:av>bv?-dir:0;});
  renderTable();
  document.getElementById('rcnt').textContent=fd.length+' из '+data.length;
}
function srt(c){
  if(col===c)dir*=-1;else{col=c;dir=-1;}
  document.querySelectorAll('thead th').forEach(th=>th.classList.remove('srt'));
  if(event&&event.target)event.target.classList.add('srt');
  applyFilters();
}

// ── Helpers ────────────────────────────────────────────────────────────
function pb(p){
  const l=(p||'').toLowerCase();
  let c='p-oth';
  if(l.includes('критич')||l==='1')c='p-crit';
  else if(l.includes('высок')||l==='2')c='p-high';
  else if(l.includes('средн')||l==='3')c='p-med';
  else if(l.includes('низк')||l==='4')c='p-low';
  return'<span class="pb '+c+'">'+(p||'—')+'</span>';
}
function e(s){return(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');}
function trunc(s,n){return(s||'').length>n?(s.slice(0,n)+'…'):s;}
function now(){return new Date().toLocaleString('ru',{day:'2-digit',month:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit'})}

// ── Table render ───────────────────────────────────────────────────────
function renderTable(){
  const isRFS=tab==='rfs';
  const cols=isRFS
    ?`<th>Статус</th><th onclick="srt('ticket')">Тикет</th><th onclick="srt('received')">Получено</th><th onclick="srt('priority')">Приоритет</th><th onclick="srt('category')">Категория</th><th>Описание</th><th onclick="srt('store')">Магазин</th>`
    :`<th onclick="srt('ticket')">Тикет</th><th onclick="srt('received')">Получено</th><th onclick="srt('priority')">Приоритет</th><th onclick="srt('category')">Категория</th><th>Описание</th><th onclick="srt('store')">Магазин</th><th onclick="srt('contact')">Контакт</th>`;
  document.getElementById('thead-row').innerHTML=cols;
  const span=isRFS?7:7;
  const tb=document.getElementById('tbody');
  if(!fd.length){tb.innerHTML=`<tr><td colspan="${span}" style="text-align:center;padding:32px;color:var(--muted)">Нет данных</td></tr>`;return;}
  tb.innerHTML=fd.map((r,i)=>{
    const st=isRFS?stBadge(getTS(r.ticket).status):'';
    return`<tr onclick="showD(${i})">
      ${isRFS?`<td>${st}</td>`:''}
      <td><span class="tn">${e(r.ticket)}</span></td>
      <td style="color:var(--muted);font-size:12px">${e(r.received)}</td>
      <td>${pb(r.priority)}</td>
      <td title="${e(r.category)}">${e(trunc(r.category,38))}</td>
      <td title="${e(r.description)}">${e(trunc(r.description,55))}</td>
      <td style="color:var(--muted)">${e(r.store)}</td>
      ${!isRFS?`<td style="color:var(--muted)">${e(r.contact)}</td>`:''}
    </tr>`;
  }).join('');
}

// ── Modal ──────────────────────────────────────────────────────────────
function showD(i){
  const r=fd[i];
  curTicket=r.ticket;
  document.getElementById('dc').innerHTML=`
    <div class="dt ${tab}">${e(r.ticket)}</div>
    <div class="d-grid">
      <div class="df"><label>Получено</label><div class="v">${e(r.received)||'—'}</div></div>
      <div class="df"><label>Дата регистрации</label><div class="v">${e(r.reg_date)||'—'}</div></div>
      <div class="df"><label>Срок решения</label><div class="v">${e(r.deadline)||'—'}</div></div>
      <div class="df"><label>Приоритет</label><div class="v">${pb(r.priority)}</div></div>
      <div class="df"><label>Магазин</label><div class="v">${e(r.store)||'—'}</div></div>
      <div class="df"><label>Контакт</label><div class="v">${e(r.contact)||'—'}${r.emp_id?' ('+e(r.emp_id)+')':''}</div></div>
    </div>
    <div class="df"><label>Категория</label><div class="v" style="font-size:13px">${e(r.category)||'—'}</div></div>
    ${r.description?'<div class="dd"><label>Описание</label><div class="v">'+e(r.description)+'</div></div>':''}
    ${r.add_info?'<div class="dd"><label>Доп. информация</label><div class="v">'+e(r.add_info)+'</div></div>':''}
  `;
  if(tab==='rfs'){
    document.getElementById('workflow').style.display='block';
    renderWorkflow();
  } else {
    document.getElementById('workflow').style.display='none';
  }
  document.getElementById('ov').classList.add('open');
}
function closeD(ev){
  if(!ev||ev.target===document.getElementById('ov')||ev.target.classList.contains('dp-close'))
    document.getElementById('ov').classList.remove('open');
}
document.addEventListener('keydown',ev=>{if(ev.key==='Escape')closeD();});

// ── Workflow ───────────────────────────────────────────────────────────
function renderWorkflow(){
  const ts=getTS(curTicket);
  const st=ts.status||'Активна';
  document.getElementById('wf-status').innerHTML=stBadge(st);
  // Reset inputs
  document.querySelectorAll('input[name="rtype"]').forEach(r=>r.checked=false);
  document.getElementById('key-sel').style.display='none';
  document.getElementById('sel-type').value='';
  document.getElementById('sel-cat').value='';
  document.getElementById('sel-key').innerHTML='<option value="">Выберите ключ...</option>';
  document.getElementById('wf-cancel-form').style.display='none';
  // Control buttons (Сбросить/Отменить)
  const showCtrl = st!=='Активна'&&st!=='Выполнено';
  document.getElementById('wf-ctrl').style.display=showCtrl?'flex':'none';
  if(showCtrl) document.getElementById('wf-cancel-btn').style.display=st==='Отменена'?'none':'inline-flex';
  // Show sections
  document.getElementById('wf-take').style.display=st==='Активна'?'block':'none';
  document.getElementById('wf-resolve').style.display=st==='В работе'?'block':'none';
  document.getElementById('wf-approval').style.display=st==='Согласование'?'block':'none';
  document.getElementById('wf-done').style.display=st==='Выполнено'?'block':'none';
  document.getElementById('wf-cancelled').style.display=st==='Отменена'?'block':'none';
  // Fill info boxes
  if(st==='Согласование'||st==='Выполнено'){
    const info=ts.resolve_type==='new'
      ?'<strong>Решение:</strong> Новый ключ'
      :`<strong>Решение:</strong> Замена — ${e(ts.key_type||'')} / ${e(ts.key_cat||'')}<br><strong>Ключ:</strong> ${e(ts.key_display||'')}`;
    if(st==='Согласование'){
      document.getElementById('wf-aprv-info').innerHTML=info+`<div class="wf-meta">Отправлено: ${e(ts.sent_at||'')}</div>`;
    } else {
      document.getElementById('wf-done-info').innerHTML=info+`<div class="wf-meta">Выполнено: ${e(ts.done_at||'')}</div>`;
    }
  }
  if(st==='Отменена'){
    document.getElementById('wf-cancelled-info').innerHTML=
      (ts.cancel_comment?`<strong>Причина:</strong> ${e(ts.cancel_comment)}<br>`:'<em style="color:var(--muted)">Причина не указана</em><br>')+
      `<div class="wf-meta">Отменено: ${e(ts.cancelled_at||'')}</div>`;
  }
}
function resetTicket(){
  setTS(curTicket,{status:'Активна',resolve_type:null,key_type:null,key_cat:null,
    key_display:null,sent_at:null,done_at:null,taken_at:null,cancel_comment:null,cancelled_at:null});
  renderWorkflow();renderTable();calcSavings();
}
function showCancelForm(){
  document.getElementById('wf-cancel-form').style.display='block';
  document.getElementById('wf-ctrl').style.display='none';
  document.getElementById('cancel-comment').value='';
  document.getElementById('cancel-comment').focus();
}
function hideCancelForm(){
  document.getElementById('wf-cancel-form').style.display='none';
  document.getElementById('wf-ctrl').style.display='flex';
}
function cancelTicket(){
  const comment=document.getElementById('cancel-comment').value.trim();
  setTS(curTicket,{status:'Отменена',cancel_comment:comment,cancelled_at:now()});
  renderWorkflow();renderTable();calcSavings();
}
function takeTicket(){
  setTS(curTicket,{status:'В работе',taken_at:now()});
  renderWorkflow();renderTable();
}
function toggleKeySel(){
  const v=document.querySelector('input[name="rtype"]:checked')?.value;
  document.getElementById('key-sel').style.display=v==='replace'?'flex':'none';
}
function updateKeyList(){
  const t=document.getElementById('sel-type').value;
  const c=document.getElementById('sel-cat').value;
  const sel=document.getElementById('sel-key');
  sel.innerHTML='<option value="">Выберите ключ...</option>';
  if(!t||!c)return;
  const sheetName=c+'_'+t;
  const keys=DATA.keys[sheetName]||[];
  keys.forEach(k=>{
    const partner=k['Партнер']||'';
    const keyCode=k['Ключ']||'';
    const expiry=k['Окончание рабочего периода ключа']||k['Окончание рабочего периода ОР']||'';
    const label=[partner,keyCode,expiry?'до '+expiry:''].filter(v=>v.trim()).join(' | ');
    if(!label)return;
    const o=document.createElement('option');
    o.value=JSON.stringify({partner,keyCode,expiry});
    o.textContent=label;
    sel.appendChild(o);
  });
  if(!keys.length){
    const o=document.createElement('option');
    o.disabled=true;o.textContent='Нет доступных ключей';
    sel.appendChild(o);
  }
}
function sendApproval(){
  const rt=document.querySelector('input[name="rtype"]:checked')?.value;
  if(!rt){alert('Выберите тип решения');return;}
  let data={status:'Согласование',resolve_type:rt,sent_at:now()};
  if(rt==='replace'){
    const kt=document.getElementById('sel-type').value;
    const kc=document.getElementById('sel-cat').value;
    const kv=document.getElementById('sel-key').value;
    if(!kt||!kc||!kv){alert('Выберите тип, категорию и ключ');return;}
    let ki;try{ki=JSON.parse(kv);}catch(err){ki={keyCode:kv};}
    data.key_type=kt;data.key_cat=kc;
    data.key_display=[ki.partner,ki.keyCode,ki.expiry?'до '+ki.expiry:''].filter(v=>v).join(' | ');
    updateKeyInSheet(kc+'_'+kt,ki.keyCode,curTicket);
  }
  setTS(curTicket,data);
  renderWorkflow();renderTable();calcSavings();
}
function updateKeyInSheet(sheetName,keyCode,ticket){
  const url=DATA.apps_script_url;
  if(!url||!keyCode)return;
  const params=new URLSearchParams({sheetName,keyCode,status:'Закрыта',ticket});
  fetch(url+'?'+params.toString())
    .then(r=>r.json())
    .then(d=>d.success?console.log('Key updated in sheet'):console.warn('Key update:',d.error))
    .catch(err=>console.warn('Sheet update error:',err));
}
function completeTicket(){
  setTS(curTicket,{status:'Выполнено',done_at:now()});
  renderWorkflow();renderTable();calcSavings();
}

// ── Savings ────────────────────────────────────────────────────────────
const PRICE={'Кухни_Лицензии':42000,'Ванные_Лицензии':53000,'Кухни_Рендер':42000,'Ванные_Рендер':42000};
const HIST={kk:12,kk_amt:504000,bk:9,bk_amt:477000};
function rub(n){return n.toLocaleString('ru-RU')+' ₽';}
function calcSavings(){
  const s=getStatuses();
  let kk=0,bk=0,kr=0,br=0;
  Object.values(s).forEach(ts=>{
    if(ts.status!=='Выполнено'||ts.resolve_type!=='replace')return;
    const tp=(ts.key_type||'').toLowerCase();
    const ct=(ts.key_cat||'').toLowerCase();
    if(tp.includes('лицензи')){if(ct.includes('кухн'))kk++;else if(ct.includes('ванн'))bk++;}
    else if(tp.includes('рендер')){if(ct.includes('кухн'))kr++;else br++;}
  });
  const tkk=HIST.kk+kk,tbk=HIST.bk+bk;
  const amtK=HIST.kk_amt+kk*PRICE['Кухни_Лицензии']+HIST.bk_amt+bk*PRICE['Ванные_Лицензии'];
  const amtR=(kr+br)*PRICE['Кухни_Рендер'];
  const grand=amtK+amtR;
  document.getElementById('sv-kk').textContent=tkk;
  document.getElementById('sv-bk').textContent=tbk;
  document.getElementById('sv-tk').textContent=tkk+tbk;
  document.getElementById('sv-kr').textContent=kr;
  document.getElementById('sv-br').textContent=br;
  document.getElementById('sv-tr').textContent=kr+br;
  document.getElementById('sv-amt-k').textContent=rub(amtK);
  document.getElementById('sv-sub-k').textContent=(tkk+tbk)+' ключей';
  document.getElementById('sv-amt-r').textContent=rub(amtR);
  document.getElementById('sv-sub-r').textContent=(kr+br)+' рендеров';
  document.getElementById('sv-grand').textContent=rub(grand);
  document.getElementById('sv-grand-sub').textContent='ключи + рендеры';
}

function renderCharts(){
  const g='rgba(255,255,255,.06)',tk='#8b949e';
  Chart.defaults.font.family="'Manrope',sans-serif";
  Chart.defaults.color=tk;

  new Chart(document.getElementById('cw'),{
    type:'bar',
    data:{
      labels:DATA.weekly.map(w=>w.label),
      datasets:[
        {label:'INC',data:DATA.weekly.map(w=>w.inc),backgroundColor:'rgba(37,99,235,.75)',borderRadius:4},
        {label:'RFS',data:DATA.weekly.map(w=>w.rfs),backgroundColor:'rgba(5,150,105,.75)',borderRadius:4}
      ]
    },
    options:{responsive:true,maintainAspectRatio:false,
      plugins:{legend:{position:'top',labels:{boxWidth:12,padding:14}}},
      scales:{x:{grid:{color:g},ticks:{color:tk}},y:{grid:{color:g},ticks:{color:tk,stepSize:1},beginAtZero:true}}}
  });

  new Chart(document.getElementById('cd'),{
    type:'line',
    data:{
      labels:DATA.daily.map(d=>d.label),
      datasets:[
        {label:'INC',data:DATA.daily.map(d=>d.inc),borderColor:'#3b82f6',backgroundColor:'rgba(59,130,246,.12)',tension:.4,fill:true,pointRadius:3,pointHoverRadius:5},
        {label:'RFS',data:DATA.daily.map(d=>d.rfs),borderColor:'#10b981',backgroundColor:'rgba(16,185,129,.12)',tension:.4,fill:true,pointRadius:3,pointHoverRadius:5},
        {label:'Итого',data:DATA.daily.map(d=>d.total),borderColor:'#f59e0b',backgroundColor:'transparent',tension:.4,borderDash:[5,3],pointRadius:2}
      ]
    },
    options:{responsive:true,maintainAspectRatio:false,
      plugins:{legend:{position:'top',labels:{boxWidth:12,padding:14}}},
      scales:{x:{grid:{color:g},ticks:{color:tk,maxTicksLimit:7}},y:{grid:{color:g},ticks:{color:tk,stepSize:1},beginAtZero:true}}}
  });
}
init();
</script>
</body>
</html>"""

    return template.replace('__DATA_JSON__', data_json)

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log('=== Запуск обновления Ceramic3D ===')

    log('Подключение к почте...')
    acc = connect_ews()

    log('Загрузка писем...')
    emails = []
    for attempt in range(1, 4):
        try:
            qs = acc.inbox.filter(
                subject__icontains='ceramic3d',
                datetime_received__gte=DATE_FROM
            ).only('subject', 'sender', 'datetime_received', 'body')
            qs.page_size = 20
            emails = list(qs)
            log(f'Загружено: {len(emails)} писем')
            break
        except Exception as e:
            log(f'Попытка {attempt}/3 не удалась: {e}')
            if attempt < 3:
                time.sleep(10 * attempt)
            else:
                raise

    parsed = [parse_email(m) for m in emails]

    seen = {}
    for p in sorted(parsed, key=lambda x: x['received_dt'] or datetime.min.replace(tzinfo=timezone.utc)):
        if p['ticket'] and p['ticket'] not in seen:
            seen[p['ticket']] = p

    deduped  = sorted(seen.values(),
                      key=lambda x: x['received_dt'] or datetime.min.replace(tzinfo=timezone.utc),
                      reverse=True)
    inc_rows = [p for p in deduped if p['type'] == 'INC']
    rfs_rows = [p for p in deduped if p['type'] == 'RFS']
    log(f'После дедупликации: {len(inc_rows)} INC, {len(rfs_rows)} RFS')

    tok = get_token()
    log('Google токен получен')

    sids = setup_sheets(tok)
    log(f'Листы: {list(sids.keys())}')

    log('Запись INC...')
    inc_sid = sids['INC — Инциденты']
    ensure_rows(tok, inc_sid, max(len(inc_rows)+50, 500))
    clear_and_write(tok, 'INC — Инциденты', [HEADERS] + [to_row(r) for r in inc_rows], 'A:K')
    format_data_sheet(tok, inc_sid, inc_rows, True)
    log(f'  INC: {len(inc_rows)} строк записано')

    log('Запись RFS...')
    rfs_sid = sids['RFS — Заявки']
    ensure_rows(tok, rfs_sid, max(len(rfs_rows)+50, 500))
    clear_and_write(tok, 'RFS — Заявки', [HEADERS] + [to_row(r) for r in rfs_rows], 'A:K')
    format_data_sheet(tok, rfs_sid, rfs_rows, False)
    log(f'  RFS: {len(rfs_rows)} строк записано')

    log('Дашборд...')
    write_dashboard(tok, sids['Дашборд'], inc_rows, rfs_rows)

    log('Загрузка ключей из таблицы...')
    keys_data = fetch_keys(tok)

    log('Генерация HTML...')
    now  = datetime.now(MSK)
    html = generate_html(inc_rows, rfs_rows, now, keys_data)
    with open(HTML_OUT, 'w', encoding='utf-8') as f:
        f.write(html)
    log(f'  HTML сохранён: {HTML_OUT}')

    log(f'=== Готово! INC:{len(inc_rows)} RFS:{len(rfs_rows)} ===')
    log(f'    https://docs.google.com/spreadsheets/d/{SHEET_ID}')

if __name__ == '__main__':
    main()
