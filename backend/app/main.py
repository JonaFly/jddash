import pandas as pd
from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine, text
import io
import os

app = FastAPI(title="BD Dashboard API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = "sqlite:///app.db"
engine = create_engine(DB_PATH)

# Initialize tables if not exist
with engine.connect() as conn:
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS records (
        stat_date DATE,
        shop_id VARCHAR(100),
        shop_name VARCHAR(255),
        bd_name VARCHAR(100),
        district VARCHAR(100),
        street VARCHAR(100),
        is_operating VARCHAR(10),
        is_b1 VARCHAR(10),
        is_b2 VARCHAR(10),
        shipping_discount FLOAT,
        first_operate_time DATE,
        actual_pay FLOAT,
        create_time DATE,
        audit_status VARCHAR(50)
    )
    """))
    conn.commit()

@app.post("/api/upload")
async def upload_excel(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        try:
            xl = pd.ExcelFile(io.BytesIO(contents))
            target_sheet = 0
            for sn in xl.sheet_names:
                if '数据' in sn and '3合一' in sn and '前一天' not in sn:
                    target_sheet = sn
                    break
            df = xl.parse(target_sheet)
        except Exception:
            df = pd.read_excel(io.BytesIO(contents))
        
        col_mapping = {
            '日期': 'stat_date',
            '门店id': 'shop_id', '商家id': 'shop_id', '门店ID': 'shop_id', '商家ID': 'shop_id',
            '门店名称': 'shop_name', '商家名称': 'shop_name',
            '业务经理': 'bd_name', 
            '区县': 'district', '区县名称': 'district',
            '街道': 'street',
            '门店是否营业': 'is_operating',
            '是否券B1活动报名': 'is_b1',
            '是否券B2新客加补活动报名': 'is_b2',
            '最低运费减免金额': 'shipping_discount',
            '门店首营时间': 'first_operate_time',
            '实付GMV': 'actual_pay',
            '门店建店日期': 'create_time',
            '门店资质审核状态': 'audit_status'
        }
        
        final_df = pd.DataFrame()
        # Strictly prefer '业务经理' for the salesperson BD field if it exists
        if '业务经理' in df.columns:
            final_df['bd_name'] = df['业务经理']
            
        for excel_col in df.columns:
            clean_col = str(excel_col).strip()
            if clean_col in col_mapping:
                target = col_mapping[clean_col]
                # If we already set bd_name from '业务经理', don't let 'BD名称' overwrite it
                if target == 'bd_name' and 'bd_name' in final_df.columns and clean_col != '业务经理':
                    continue
                if target in final_df.columns:
                    if df[excel_col].count() > final_df[target].count():
                        final_df[target] = df[excel_col]
                else:
                    final_df[target] = df[excel_col]
        df = final_df
        
        db_cols = ['stat_date', 'shop_id', 'shop_name', 'bd_name', 'district', 'street', 
                   'is_operating', 'is_b1', 'is_b2', 'shipping_discount', 
                   'first_operate_time', 'actual_pay', 'create_time', 'audit_status']
        for col in db_cols:
            if col not in df.columns:
                df[col] = None
        df = df[db_cols]

        df['stat_date'] = pd.to_datetime(df['stat_date'], errors='coerce').dt.date
        df['first_operate_time'] = pd.to_datetime(df['first_operate_time'], errors='coerce').dt.date
        df['create_time'] = pd.to_datetime(df['create_time'], errors='coerce').dt.date
        df['shipping_discount'] = pd.to_numeric(df['shipping_discount'], errors='coerce').fillna(0)
        df['actual_pay'] = pd.to_numeric(df['actual_pay'], errors='coerce').fillna(0)
        
        df = df.dropna(subset=['stat_date'])
        
        uploaded_dates = df['stat_date'].unique().tolist()
        if uploaded_dates:
            with engine.begin() as conn:
                dates_str = ",".join([f"'{d}'" for d in uploaded_dates])
                conn.execute(text(f"DELETE FROM records WHERE stat_date IN ({dates_str})"))
        
        df.to_sql('records', con=engine, if_exists='append', index=False)
        return {"status": "ok", "message": f"Successfully imported {len(df)} rows into database."}
    except Exception as e:
        return {"status": "error", "message": f"Upload Failed: {str(e)}"}

@app.post("/api/upload/store")
async def upload_store(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents))
        return {"status": "ok", "message": f"Successfully imported store details: {len(df)} rows."}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/dashboard/stats")
def get_dashboard_stats():
    try:
        df = pd.read_sql('SELECT * FROM records', con=engine)
    except Exception:
        df = pd.DataFrame()
        
    if df.empty:
        return {"status": "ok", "data": None}

    df['stat_date'] = pd.to_datetime(df['stat_date'], errors='coerce')
    df['first_operate_time'] = pd.to_datetime(df['first_operate_time'], errors='coerce')
    df['create_time'] = pd.to_datetime(df['create_time'], errors='coerce')
    
    max_date = df['stat_date'].max()
    yesterday_date = max_date - pd.Timedelta(days=1)
    current_month = max_date.replace(day=1)
    
    today_df = df[df['stat_date'] == max_date]
    month_df = df[df['stat_date'] >= current_month]

    total_gmv = float(df['actual_pay'].sum())
    shops = len(today_df[today_df['shop_id'].notnull()]['shop_id'].unique())
    if shops == 0:
        shops = len(today_df['shop_name'].unique()) if not today_df.empty else 0
    
    total_new_shops_this_month = today_df[(today_df['create_time'] >= current_month) & (today_df['audit_status'].astype(str).str.contains('审核通过'))]['shop_id'].nunique()
    total_new_gmv_this_month = month_df[month_df['first_operate_time'] >= current_month]['actual_pay'].sum()

    b1_count = int(((today_df['is_operating'] == '是') & (today_df['is_b1'] == '是')).sum())
    b2_count = int(((today_df['is_operating'] == '是') & (today_df['is_b2'] == '是')).sum())
    fs_count = int(((today_df['is_operating'] == '是') & (today_df['shipping_discount'] >= 2.7)).sum())
    other_count = int(max(0, len(today_df[today_df['is_operating'] == '是']) - b1_count))

    df_date_grouped = df.groupby(df['stat_date'].dt.strftime('%m-%d')).agg({
        'actual_pay': 'sum',
        'shop_id': 'nunique'
    }).reset_index().sort_values('stat_date').tail(14)

    def calc_rates(grp):
        base_shops = len(grp)
        if base_shops == 0: return 0, 0, 0, 0
        ops_grp = grp[grp['is_operating'] == '是']
        ops = len(ops_grp)
        op_rate = ops / base_shops if base_shops > 0 else 0
        ops_b1 = (ops_grp['is_b1'] == '是').sum()
        b1_rate = ops_b1 / ops if ops > 0 else 0
        ops_b2 = (ops_grp['is_b2'] == '是').sum()
        b2_rate = ops_b2 / ops if ops > 0 else 0
        ops_fs = (ops_grp['shipping_discount'] >= 2.7).sum()
        fs_rate = ops_fs / ops if ops > 0 else 0
        return op_rate, b1_rate, b2_rate, fs_rate

    def get_row_data(name_key, dim_val, group):
        today_group = group[group['stat_date'] == max_date]
        yest_group = group[group['stat_date'] == yesterday_date]
        month_group = group[group['stat_date'] >= current_month]
        
        month_gmv = month_group['actual_pay'].sum()
        total_shops = today_group['shop_id'].nunique() if today_group['shop_id'].notnull().any() else today_group['shop_name'].nunique()
        
        new_shops_today = today_group[(today_group['create_time'] >= current_month) & (today_group['audit_status'].astype(str).str.contains('审核通过'))]['shop_id'].nunique()
        new_shops_yest = yest_group[(yest_group['create_time'] >= current_month) & (yest_group['audit_status'].astype(str).str.contains('审核通过'))]['shop_id'].nunique()
        new_shops_this_month = new_shops_today
        yesterday_new_shops = new_shops_today - new_shops_yest
        
        new_gmv_this_month = month_group[month_group['first_operate_time'] >= current_month]['actual_pay'].sum()
        yesterday_new_gmv = yest_group['actual_pay'].sum() if not yest_group.empty else 0
        
        op_r_t, b1_r_t, b2_r_t, fs_r_t = calc_rates(today_group)
        op_r_y, b1_r_y, b2_r_y, fs_r_y = calc_rates(yest_group)
        
        return {
            name_key: str(dim_val),
            "yesterday_new_shops": int(yesterday_new_shops),
            "yesterday_new_gmv": round(float(yesterday_new_gmv), 2),
            "yesterday_b1_rate": f"{(b1_r_t - b1_r_y)*100:+.2f}%", 
            "yesterday_b2_gt2_rate": f"{(op_r_t - op_r_y)*100:+.2f}%", 
            "yesterday_free_shipping_rate": f"{(fs_r_t - fs_r_y)*100:+.2f}%", 
            "month_new_shops": int(new_shops_this_month),
            "month_new_gmv": round(float(new_gmv_this_month), 2),
            "total_shops": int(total_shops),
            "month_gmv": round(float(month_gmv), 2),
            "operate_rate": f"{op_r_t*100:.2f}%",
            "operate_sales_rate": "0.00%", 
            "month_b1_rate": f"{b1_r_t*100:.2f}%", 
            "month_b2_rate": f"{b2_r_t*100:.2f}%", 
            "month_b2_gt2_rate": "-", 
            "month_free_shipping_rate": f"{fs_r_t*100:.2f}%", 
            "target_completion_rate": "-" 
        }

    def build_dimension_table(dimension_col, name_key):
        table_data = []
        if dimension_col not in df.columns: return table_data
        for dim_val, group in df.groupby(dimension_col):
            if pd.isna(dim_val) or str(dim_val).strip() == '': continue
            row = get_row_data(name_key, dim_val, group)
            table_data.append(row)
        return table_data
        
    def build_tree_table():
        tree_data = []
        if 'district' not in df.columns or 'street' not in df.columns: return tree_data
        uid = 0
        for dist_name, d_group in df.groupby('district'):
            if pd.isna(dist_name) or str(dist_name).strip() == '': continue
            uid += 1
            row_d = get_row_data('street', dist_name, d_group)
            row_d['id'] = uid
            children = []
            for st_name, s_group in d_group.groupby('street'):
                if pd.isna(st_name) or str(st_name).strip() == '': continue
                uid += 1
                row_s = get_row_data('street', st_name, s_group)
                row_s['id'] = uid
                children.append(row_s)
            row_d['children'] = children
            if len(children) > 0: tree_data.append(row_d)
        return tree_data

    bd_table_data = build_dimension_table('bd_name', 'bd_name')
    street_table_data = build_tree_table()
    
    bd_names, bd_values = [], []
    if 'bd_name' in df.columns:
        bd_grouped = month_df.groupby('bd_name')['actual_pay'].sum().reset_index().sort_values('actual_pay', ascending=False).head(10)
        bd_names = bd_grouped['bd_name'].tolist()
        bd_values = [int(v) for v in bd_grouped['actual_pay'].tolist()]

    return {
        "status": "ok",
        "data": {
            "kpis": [
                { "title": '当前累计营业商户', "val": str(shops), "trend": 0, "unit": '家' },
                { "title": '当前累计总GMV', "val": f"{total_gmv:,.0f}", "trend": 0, "unit": '元' },
                { "title": '当月新签营业数', "val": str(total_new_shops_this_month), "trend": 0, "unit": '家' },
                { "title": '当月新签GMV', "val": f"{total_new_gmv_this_month:,.0f}", "trend": 0, "unit": '元' },
            ],
            "dates": df_date_grouped['stat_date'].tolist(),
            "newShops": df_date_grouped['shop_id'].tolist(),
            "gmvs": df_date_grouped['actual_pay'].tolist(),
            "bdRank": {"names": bd_names, "values": bd_values},
            "activityDist": [
                {"value": b1_count, "name": 'B1活动'},
                {"value": b2_count, "name": 'B2活动'},
                {"value": fs_count, "name": '免运活动'},
                {"value": other_count, "name": '其他'}
            ],
            "bdTableData": bd_table_data,
            "streetTableData": street_table_data
        }
    }

app.mount("/", StaticFiles(directory="static", html=True), name="static")
