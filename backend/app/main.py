import pandas as pd
from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine, text
import io, datetime

app = FastAPI(title="BD Dashboard API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

DB_PATH = "sqlite:///app.db"
engine = create_engine(DB_PATH)

# ─── Initialize 3 tables ───
with engine.connect() as conn:
    # Table 1: Store master (from 影刀-商户详情) - imported once
    conn.execute(text("""CREATE TABLE IF NOT EXISTS stores (
        shop_id TEXT PRIMARY KEY, shop_name TEXT,
        province TEXT, city TEXT, district TEXT, street TEXT, address TEXT
    )"""))
    # Table 2: BD mapping (from BD sheet) - imported once
    conn.execute(text("""CREATE TABLE IF NOT EXISTS bd_map (
        shop_id TEXT PRIMARY KEY, bd_name TEXT
    )"""))
    # Table 3: Daily channel data (from 渠道/daily file) - imported daily
    conn.execute(text("""CREATE TABLE IF NOT EXISTS daily (
        stat_date DATE, shop_id TEXT, shop_name TEXT,
        is_operating TEXT, is_b1 TEXT, is_b2 TEXT,
        shipping_discount REAL, first_operate_time DATE, actual_pay REAL,
        create_time DATE, audit_status TEXT
    )"""))
    conn.commit()

# ─── Upload 1: Store master + BD mapping (from 11.xlsx) ───
@app.post("/api/upload/master")
async def upload_master(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        xl = pd.ExcelFile(io.BytesIO(contents))
        msgs = []

        # --- 影刀-商户详情 ---
        if '影刀-商户详情' in xl.sheet_names:
            df = xl.parse('影刀-商户详情')
            df.columns = [str(c).strip() for c in df.columns]
            store_df = pd.DataFrame()
            store_df['shop_id'] = df['门店ID'].astype(str).str.strip()
            store_df['shop_name'] = df.get('门店名称', '')
            store_df['province'] = df.get('省', '')
            store_df['city'] = df.get('市', '')
            store_df['district'] = df.get('区县', '')
            store_df['street'] = df.get('街道', '')
            store_df['address'] = df.get('详细地址', '')
            for c in store_df.select_dtypes(include=['object']).columns:
                store_df[c] = store_df[c].astype(str).str.strip()
                store_df.loc[store_df[c].isin(['nan','None','']), c] = None
            store_df = store_df.dropna(subset=['shop_id'])
            store_df = store_df.drop_duplicates(subset=['shop_id'])
            with engine.begin() as conn:
                conn.execute(text("DELETE FROM stores"))
            store_df.to_sql('stores', con=engine, if_exists='append', index=False)
            msgs.append(f"门店主数据: {len(store_df)} 条")

        # --- BD sheet ---
        if 'BD' in xl.sheet_names:
            df = xl.parse('BD')
            df.columns = [str(c).strip() for c in df.columns]
            bd_df = pd.DataFrame()
            bd_df['shop_id'] = df['门店id'].astype(str).str.strip()
            bd_raw = df['BD'].astype(str).str.strip()
            # Remove ERP suffix like (ext.xxx)
            bd_df['bd_name'] = bd_raw.str.replace(r'\(.*?\)', '', regex=True).str.strip()
            bd_df.loc[bd_df['bd_name'].isin(['nan','None','']), 'bd_name'] = None
            bd_df = bd_df.dropna(subset=['shop_id', 'bd_name'])
            bd_df = bd_df.drop_duplicates(subset=['shop_id'], keep='last')
            with engine.begin() as conn:
                conn.execute(text("DELETE FROM bd_map"))
            bd_df.to_sql('bd_map', con=engine, if_exists='append', index=False)
            msgs.append(f"BD映射: {len(bd_df)} 条, {bd_df['bd_name'].nunique()} 个BD")

        return {"status": "ok", "message": "导入成功! " + "; ".join(msgs)}
    except Exception as e:
        import traceback
        return {"status": "error", "message": traceback.format_exc()}

# ─── Upload 2: Daily channel data (渠道 file, uploaded daily) ───
@app.post("/api/upload")
async def upload_daily(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        xl = pd.ExcelFile(io.BytesIO(contents))

        # Find the right sheet: prefer '渠道', or first sheet with '日期' column  
        target_sheet = None
        if '渠道' in xl.sheet_names:
            target_sheet = '渠道'
        else:
            for sn in xl.sheet_names:
                test = xl.parse(sn, nrows=1)
                test.columns = [str(c).strip() for c in test.columns]
                if '日期' in test.columns and '门店id' in test.columns:
                    target_sheet = sn
                    break
        if not target_sheet:
            target_sheet = xl.sheet_names[0]

        df = xl.parse(target_sheet)
        df.columns = [str(c).strip() for c in df.columns]

        w = pd.DataFrame()
        w['stat_date'] = pd.to_datetime(df.get('日期'), errors='coerce').dt.date
        w['shop_id'] = df.get('门店id', df.get('商家id', '')).astype(str).str.strip()
        w['shop_name'] = df.get('门店名称', df.get('商家名称', ''))
        w['is_operating'] = df.get('门店是否营业', '')
        w['is_b1'] = df.get('是否券B1活动报名', '')
        w['is_b2'] = df.get('是否券B2新客加补活动报名', '')
        w['shipping_discount'] = pd.to_numeric(df.get('最低运费减免金额', 0), errors='coerce').fillna(0)
        w['first_operate_time'] = pd.to_datetime(df.get('门店首营时间'), errors='coerce').dt.date
        w['actual_pay'] = pd.to_numeric(df.get('实付GMV', 0), errors='coerce').fillna(0)
        w['create_time'] = pd.to_datetime(df.get('门店建店日期'), errors='coerce').dt.date
        w['audit_status'] = df.get('门店资质审核状态', '')

        for c in w.select_dtypes(include=['object']).columns:
            w[c] = w[c].astype(str).str.strip()
            w.loc[w[c].isin(['nan','None','']), c] = None

        w = w.dropna(subset=['stat_date'])

        with engine.begin() as conn:
            for d in w['stat_date'].unique():
                conn.execute(text(f"DELETE FROM daily WHERE stat_date = '{d}'"))
        w.to_sql('daily', con=engine, if_exists='append', index=False)

        return {"status": "ok", "message": f"渠道日报导入成功! [{target_sheet}] {len(w)} 行, 日期: {list(w['stat_date'].unique())}"}
    except Exception as e:
        import traceback
        return {"status": "error", "message": traceback.format_exc()}

# ─── Dashboard Stats: JOIN daily + bd_map + stores ───
@app.get("/api/dashboard/stats")
def get_dashboard_stats():
    try:
        daily = pd.read_sql("SELECT * FROM daily", con=engine)
        bd = pd.read_sql("SELECT * FROM bd_map", con=engine)
        stores = pd.read_sql("SELECT shop_id, district, street FROM stores", con=engine)
    except:
        return {"status": "ok", "data": None}

    if daily.empty:
        return {"status": "ok", "data": None}

    # VLOOKUP: Join BD and address onto daily data
    daily['shop_id'] = daily['shop_id'].astype(str).str.strip()
    bd['shop_id'] = bd['shop_id'].astype(str).str.strip()
    stores['shop_id'] = stores['shop_id'].astype(str).str.strip()

    df = daily.merge(bd, on='shop_id', how='left')
    df = df.merge(stores, on='shop_id', how='left')

    df['stat_date'] = pd.to_datetime(df['stat_date'])
    df['create_time'] = pd.to_datetime(df['create_time'])
    df['first_operate_time'] = pd.to_datetime(df['first_operate_time'])

    max_date = df['stat_date'].max()
    ms = max_date.replace(day=1)
    yd = max_date - datetime.timedelta(days=1)

    td = df[df['stat_date'] == max_date]
    md = df[df['stat_date'] >= ms]
    yd_df = df[df['stat_date'] == yd]

    # Active BDs: those with stores on the latest day
    active_bds = [b for b in td['bd_name'].dropna().unique() if b and str(b) not in ('None','nan','')]

    # New-sign pool: shops built this month + audit passed
    def signed_pool(d):
        if d.empty: return pd.DataFrame()
        m = d['stat_date'].iloc[0].replace(day=1)
        return d[(d['create_time'] >= m) & (d['audit_status'].astype(str).str.contains('审核通过'))]

    # Rate calculator for a group of shops
    def calc_rates(grp):
        total = len(grp)
        if total == 0:
            return 0, 0, 0, 0
        ops = grp[grp['is_operating'] == '是']
        n_ops = len(ops)
        op_rate = n_ops / total
        b1_rate = (ops['is_b1'] == '是').sum() / n_ops if n_ops > 0 else 0
        b2_rate = (ops['is_b2'] == '是').sum() / n_ops if n_ops > 0 else 0
        fs_rate = (ops['shipping_discount'] >= 2.7).sum() / n_ops if n_ops > 0 else 0
        return op_rate, b1_rate, b2_rate, fs_rate

    # Format rate change as "+X.XX%" or "-X.XX%"
    def fmt_rate_delta(today_rate, yest_rate):
        delta = (today_rate - yest_rate) * 100
        return f"{delta:+.2f}%"

    # Build a full row of data for any dimension (BD or district/street)
    def build_row(today_grp, yest_grp, month_grp):
        # Daily new signs = INCREMENT: today's pool - yesterday's pool
        ns_today = signed_pool(today_grp)
        ns_yest = signed_pool(yest_grp)

        today_ids = set(ns_today['shop_id'].tolist()) if not ns_today.empty else set()
        yest_ids = set(ns_yest['shop_id'].tolist()) if not ns_yest.empty else set()
        new_today_ids = today_ids - yest_ids  # shops that are NEW today

        daily_new_shops = len(new_today_ids)
        if new_today_ids and not ns_today.empty:
            daily_new_gmv = round(float(ns_today[ns_today['shop_id'].isin(new_today_ids)]['actual_pay'].sum()), 2)
        else:
            daily_new_gmv = 0

        # Month cumulative new signs (total pool size on latest day)
        month_new_shops = len(today_ids)
        month_new_gmv = round(float(ns_today['actual_pay'].sum()), 2) if not ns_today.empty else 0

        total_shops = len(today_grp)
        month_gmv = round(float(month_grp['actual_pay'].sum()), 2) if not month_grp.empty else 0

        # Today's rates
        op_t, b1_t, b2_t, fs_t = calc_rates(today_grp)
        # Yesterday's rates (for day-over-day change)
        op_y, b1_y, b2_y, fs_y = calc_rates(yest_grp) if not yest_grp.empty else (0, 0, 0, 0)

        return {
            "yesterday_new_shops": daily_new_shops,
            "yesterday_new_gmv": daily_new_gmv,
            "yesterday_b1_rate": fmt_rate_delta(b1_t, b1_y),
            "yesterday_b2_gt2_rate": "-",   # skip for now
            "yesterday_free_shipping_rate": fmt_rate_delta(fs_t, fs_y),
            "month_new_shops": month_new_shops,
            "month_new_gmv": month_new_gmv,
            "total_shops": total_shops,
            "month_gmv": month_gmv,
            "operate_rate": f"{op_t*100:.2f}%",
            "operate_sales_rate": "-",      # skip for now
            "month_b1_rate": f"{b1_t*100:.2f}%",
            "month_b2_rate": f"{b2_t*100:.2f}%",
            "month_b2_gt2_rate": "-",       # skip for now
            "month_free_shipping_rate": f"{fs_t*100:.2f}%",
            "target_completion_rate": "-"
        }

    # ── Global KPIs ──
    shops = len(td[td['is_operating'] == '是'])
    gmv = float(md['actual_pay'].sum())
    ns_pool = signed_pool(td)
    ns = len(ns_pool)
    ng = round(float(ns_pool['actual_pay'].sum()), 2) if not ns_pool.empty else 0

    # ── Trend chart ──
    dates = sorted(df['stat_date'].unique())[-14:]
    cd, cn, cg = [], [], []
    for i, d in enumerate(dates):
        dd = df[df['stat_date'] == d]
        cd.append(pd.Timestamp(d).strftime('%m-%d'))
        cg.append(float(dd['actual_pay'].sum()))
        pt = len(signed_pool(dd))
        if i > 0:
            py = len(signed_pool(df[df['stat_date'] == dates[i-1]]))
            cn.append(max(0, pt - py))
        else:
            cn.append(0)

    # ── BD Ranking (active only) ──
    rk = md[md['bd_name'].isin(active_bds)].groupby('bd_name')['actual_pay'].sum().reset_index()
    rk = rk.sort_values('actual_pay', ascending=False).head(10)

    # ── BD Detail Table ──
    bt = []
    for bn in active_bds:
        bg = df[df['bd_name'] == bn]
        tg = bg[bg['stat_date'] == max_date]
        yg = bg[bg['stat_date'] == yd]
        mg = bg[bg['stat_date'] >= ms]
        row = build_row(tg, yg, mg)
        row["bd_name"] = bn
        bt.append(row)

    # ── District/Street Tree ──
    st = []
    uid = 0
    td_loc = td.dropna(subset=['district'])
    for dn, dg_today in td_loc.groupby('district'):
        if not dn or str(dn) in ('None','nan',''): continue
        uid += 1
        dg_yest = yd_df[yd_df['district'] == dn] if not yd_df.empty else pd.DataFrame()
        dg_month = md[md['district'] == dn]
        row_d = build_row(dg_today, dg_yest, dg_month)
        row_d["id"] = uid
        row_d["street"] = str(dn)
        row_d["children"] = []
        for sn_name, sg_today in dg_today.groupby('street'):
            if not sn_name or str(sn_name) in ('None','nan',''): continue
            uid += 1
            sg_yest = yd_df[(yd_df['district'] == dn) & (yd_df['street'] == sn_name)] if not yd_df.empty else pd.DataFrame()
            sg_month = md[(md['district'] == dn) & (md['street'] == sn_name)]
            row_s = build_row(sg_today, sg_yest, sg_month)
            row_s["id"] = uid
            row_s["street"] = str(sn_name)
            row_d["children"].append(row_s)
        st.append(row_d)

    # ── 营业对比: Non-operating shop analysis per BD ──
    # All approved shops on latest day
    approved_td = td[td['audit_status'].astype(str).str.contains('审核通过')]
    not_operating = approved_td[approved_td['is_operating'] != '是']

    # New shops: built this month; Old shops: built before this month
    new_not_op = not_operating[not_operating['create_time'] >= ms]
    old_not_op = not_operating[not_operating['create_time'] < ms]

    # Calculate consecutive non-operating days for each non-operating shop
    all_dates_sorted = sorted(df['stat_date'].unique(), reverse=True)  # newest first
    non_op_details = []

    for _, shop_row in not_operating.iterrows():
        sid = shop_row['shop_id']
        # Look through historical data for this shop to find last operating day
        consec_days = 1  # at least today
        for i, hist_date in enumerate(all_dates_sorted):
            if hist_date == max_date:
                continue  # skip today, we already know it's not operating
            hist_row = df[(df['shop_id'] == sid) & (df['stat_date'] == hist_date)]
            if hist_row.empty:
                continue  # no data for this date, skip
            if hist_row.iloc[0]['is_operating'] == '是':
                # Found the last operating day
                consec_days = (max_date - hist_date).days
                break
            else:
                consec_days = (max_date - hist_date).days + 1

        non_op_details.append({
            'shop_id': str(sid),
            'shop_name': str(shop_row.get('shop_name', '')),
            'bd_name': str(shop_row.get('bd_name', '')),
            'is_new': shop_row['create_time'] >= ms if pd.notna(shop_row['create_time']) else False,
            'consec_days': consec_days
        })

    # Build the BD-level tree for 营业对比
    biz_compare = []
    for bn in active_bds:
        bd_details = [d for d in non_op_details if d['bd_name'] == bn]
        new_count = sum(1 for d in bd_details if d['is_new'])
        old_count = sum(1 for d in bd_details if not d['is_new'])
        # Calculate operating rate for this BD
        bd_td = td[td['bd_name'] == bn]
        bd_ops = len(bd_td[bd_td['is_operating'] == '是'])
        bd_total = len(bd_td)
        op_rate = f"{bd_ops/bd_total*100:.2f}%" if bd_total > 0 else "0.00%"

        children = [{
            'shop_id': d['shop_id'],
            'shop_name': d['shop_name'],
            'bd_name': d['bd_name'],
            'consec_days': d['consec_days'],
            'is_new': '新店' if d['is_new'] else '老店'
        } for d in bd_details]
        # Sort by consec_days descending (riskiest first)
        children.sort(key=lambda x: x['consec_days'], reverse=True)

        biz_compare.append({
            'bd_name': bn,
            'new_count': new_count,
            'old_count': old_count,
            'operate_rate': op_rate,
            'children': children
        })

    return {
        "status": "ok",
        "data": {
            "kpis": [
                {"title": "当前累计营业商户", "val": str(shops), "unit": "家"},
                {"title": "当月累计总GMV", "val": f"{gmv:,.0f}", "unit": "元"},
                {"title": "当月新签营业数", "val": str(ns), "unit": "家"},
                {"title": "当月新签GMV", "val": f"{ng:,.0f}", "unit": "元"},
            ],
            "dates": cd, "newShops": cn, "gmvs": cg,
            "bdRank": {"names": rk['bd_name'].tolist(), "values": [int(v) for v in rk['actual_pay'].tolist()]},
            "bizCompare": biz_compare,
            "bdTableData": bt, "streetTableData": st
        }
    }

app.mount("/", StaticFiles(directory="static", html=True), name="static")

