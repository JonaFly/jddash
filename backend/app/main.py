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

    def signed_pool(d):
        if d.empty: return pd.DataFrame()
        m = d['stat_date'].iloc[0].replace(day=1)
        return d[(d['create_time'] >= m) & (d['audit_status'].astype(str).str.contains('审核通过'))]

    shops = len(td[td['is_operating'] == '是'])
    gmv = float(md['actual_pay'].sum())
    ns = len(signed_pool(td))
    ng = float(md[md['first_operate_time'] >= ms]['actual_pay'].sum())

    # Trend chart
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

    # BD Ranking (active only)
    rk = md[md['bd_name'].isin(active_bds)].groupby('bd_name')['actual_pay'].sum().reset_index()
    rk = rk.sort_values('actual_pay', ascending=False).head(10)

    # Activity Distribution
    ops_td = td[td['is_operating'] == '是']
    b1c = int((ops_td['is_b1'] == '是').sum())
    b2c = int((ops_td['is_b2'] == '是').sum())
    fsc = int((ops_td['shipping_discount'] >= 2.7).sum())

    # BD Detail Table
    bt = []
    for bn in active_bds:
        bg = df[df['bd_name'] == bn]
        tg = bg[bg['stat_date'] == max_date]
        mg = bg[bg['stat_date'] >= ms]
        yg = bg[bg['stat_date'] == yd]
        ms_t = len(signed_pool(tg))
        ms_y = len(signed_pool(yg))
        ot = tg[tg['is_operating'] == '是']
        opr = len(ot)/len(tg) if len(tg) > 0 else 0
        b1r = (ot['is_b1'] == '是').sum()/len(ot) if len(ot) > 0 else 0
        b2r = (ot['is_b2'] == '是').sum()/len(ot) if len(ot) > 0 else 0
        fsr = (ot['shipping_discount'] >= 2.7).sum()/len(ot) if len(ot) > 0 else 0
        bt.append({
            "bd_name": bn,
            "yesterday_new_shops": max(0, ms_t - ms_y),
            "yesterday_new_gmv": round(float(yg['actual_pay'].sum()), 2) if not yg.empty else 0,
            "month_new_shops": ms_t,
            "total_shops": len(tg),
            "month_gmv": round(float(mg['actual_pay'].sum()), 2),
            "operate_rate": f"{opr*100:.2f}%",
            "month_b1_rate": f"{b1r*100:.2f}%",
            "yesterday_b1_rate": "+0.00%",
            "month_b2_rate": f"{b2r*100:.2f}%",
            "month_free_shipping_rate": f"{fsr*100:.2f}%",
            "target_completion_rate": "-"
        })

    # District/Street Tree (from stores table join)
    st = []
    uid = 0
    td_with_loc = td.dropna(subset=['district'])
    for dn, dg in td_with_loc.groupby('district'):
        if not dn or str(dn) in ('None','nan',''): continue
        uid += 1
        dmg = df[(df['district'] == dn) & (df['stat_date'] >= ms)]
        dop = len(dg[dg['is_operating'] == '是'])/len(dg) if len(dg) > 0 else 0
        row = {"id": uid, "street": str(dn), "total_shops": len(dg),
               "month_gmv": round(float(dmg['actual_pay'].sum()), 2),
               "operate_rate": f"{dop*100:.2f}%",
               "month_new_shops": len(signed_pool(dg)), "children": []}
        for sn_name, sg in dg.groupby('street'):
            if not sn_name or str(sn_name) in ('None','nan',''): continue
            uid += 1
            smg = df[(df['street'] == sn_name) & (df['stat_date'] >= ms)]
            sop = len(sg[sg['is_operating'] == '是'])/len(sg) if len(sg) > 0 else 0
            row['children'].append({"id": uid, "street": str(sn_name), "total_shops": len(sg),
                "month_gmv": round(float(smg['actual_pay'].sum()), 2),
                "operate_rate": f"{sop*100:.2f}%",
                "month_new_shops": len(signed_pool(sg))})
        st.append(row)

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
            "activityDist": [
                {"value": b1c, "name": "B1活动"}, {"value": b2c, "name": "B2活动"},
                {"value": fsc, "name": "免运活动"}, {"value": max(0, shops-b1c), "name": "其他"}
            ],
            "bdTableData": bt, "streetTableData": st
        }
    }

app.mount("/", StaticFiles(directory="static", html=True), name="static")
