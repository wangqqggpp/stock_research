# coding:utf-8
import datetime
import numpy as np
import pandas as pd
import tushare as ts
import MySQLdb
from datetime import date,time,timedelta

class Stock(object):
    '''同步tuShare数据到本地数据库方法 '''
    def __init__(self):
        # 设置tushare pro的token并获取连接
        ts.set_token('**************************************************************')
    @classmethod
    def mysql_conn(cls):
        # 数据库初始连接 commit() rollback() cursor() fetchall() close()
        db = MySQLdb.connect(host='127.0.0.1', port=3306, user='scott', passwd='*****', db='stock', charset='utf8')
        return db
    def mysql_select(cls,select_sql):
        # 数据库初始连接 commit() rollback() cursor() fetchall() close()
        db = MySQLdb.connect(host='127.0.0.1', port=3306, user='scott', passwd='*****', db='stock', charset='utf8')
        cursor = db.cursor()
        cursor.execute(select_sql)
        res = cursor.fetchall()
        return res

        return db
    def stock_basic(self):
        pro = ts.pro_api()
        db = Stock.mysql_conn()
        cursor = db.cursor()
        # 查询当前所有正常上市交易的股票列表
        df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,market,exchange,list_date,is_hs')
        df.drop_duplicates()
        try:
            rows, cols = df.shape
            col_names = df.columns
            # 循环提取返回字段 入库
            for row in np.arange(0, rows):
                ts_code = df.ix[row, col_names[0]]
                symbol = df.ix[row, col_names[1]]
                name = df.ix[row, col_names[2]]
                area = df.ix[row, col_names[3]]
                industry = df.ix[row, col_names[4]]
                market = df.ix[row, col_names[5]]
                exchange = df.ix[row, col_names[6]]
                list_date = (datetime.datetime.strptime(df.ix[row, col_names[7]], "%Y%m%d")).strftime('%Y-%m-%d')
                is_hs = df.ix[row, col_names[8]]
                # 入库字段 字符串化
                lisa = [str(ele) for ele in
                        [ts_code,symbol,name,area,industry,market,exchange,list_date,is_hs]]
                # 数据入库
                sql_insert = """
                        insert into stock_basic
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                cursor.execute(sql_insert, (
                lisa[0], lisa[1], lisa[2], lisa[3], lisa[4], lisa[5], lisa[6], lisa[7], lisa[8]))
            # 数据提交
            db.commit()
        except :
            print(Exception("未知错误"))
    def stock_daily(self,ts_code,start_date, end_date):
        pro = ts.pro_api()
        db = Stock.mysql_conn()
        cursor = db.cursor()
        # 设定获取日线行情的初始日期和终止日期，其中终止日期设定为昨天。
        try:
            df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
            rows, cols = df.shape
            col_names = df.columns
            for row in np.arange(0, rows):
                ts_code = df.ix[row, col_names[0]]
                trade_date = (datetime.datetime.strptime(df.ix[row, col_names[1]], "%Y%m%d")).strftime('%Y-%m-%d')
                open = df.ix[row, col_names[2]]
                high = df.ix[row, col_names[3]]
                low = df.ix[row, col_names[4]]
                close = df.ix[row, col_names[5]]
                pre_close = df.ix[row, col_names[6]]
                change = df.ix[row, col_names[7]]
                pct_chg = df.ix[row, col_names[8]]
                vol = df.ix[row, col_names[9]]
                amount = df.ix[row, col_names[10]]
                lisa = [str(ele) for ele in
                        [ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount]]

                sql_insert = """
                            insert into stock_daily
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                cursor.execute(sql_insert, (
                lisa[0], lisa[1], lisa[2], lisa[3], lisa[4], lisa[5], lisa[6], lisa[7], lisa[8], lisa[9], lisa[10]))
            db.commit()
        except Exception:
            print("error")

class Data_init(object):
    '''数据表数据更新'''
	def __init__(self):
        # 设置tushare pro的token并获取连接
        ts.set_token('**************************************************************')
    def stock_daily_init(self):
        get_data = Get_data()
        res_list = get_data.stock_pool()
        # 获取日数据 最新时间
        [year, month, day] = [int(ele) for ele in get_data.max_day().split("-")]
        start_date, end_date = date(year, month, day) + timedelta(1), date.today()
        # 获取 股票池中股票代码
        get_data = Get_data()
        res_list = get_data.stock_pool()
        # 更新数据
        if start_date == end_date | start_date == None:
            for res in res_list:
                ts_code = str(res[0])
                Stock().stock_daily(ts_code=str(ts_code))
        else:
            for res in res_list:
                ts_code = str(res[0])
                Stock().stock_daily(ts_code=str(ts_code), start_date=str(start_date.strftime("%Y%m%d")), end_date=str(end_date.strftime("%Y%m%d")))

class Get_data(object):
    '''查询数据'''
    def __init__(self):
        # 设置tushare pro的token并获取连接
        ts.set_token('**************************************************************')
    def stock_pool(self):
        select_sql = """
            SELECT t.`ts_code`,t.`symbol`,t.`name`
                FROM stock_basic t
                WHERE t.symbol IN ("601169","600028","601216","601872","002024","601866","601919","601018","601857","601985","600170","601611","601009","600887","600606","601899")
                ORDER BY t.symbol ;
            """
        res = Stock().mysql_select(select_sql=select_sql)
        return res
    def max_day(self):
        select_sql = """
            SELECT max(trade_date) from stock_daily;
             """
        max_day = Stock().mysql_select(select_sql=select_sql)
        return max_day[0][0]
    def stock_daily(self):
        select_sql = """
            SELECT a.ts_code,b.name,a.trade_date,a.open,a.close,a.vol,a.amount
            from stock_daily a,stock_basic b
            where a.ts_code = b.ts_code;
            """
        res = Stock().mysql_select(select_sql=select_sql)
        res_frame = pd.DataFrame(data=np.array(res),
                                 columns=["ts_code", "name", "trade_date", "open", "close", "vol", "amount"])
        return res_frame
    def stock_vol_match_price(self):
        get_data = Get_data()
        res_frame = get_data.stock_daily()
        colse_frame = res_frame.pivot(index="trade_date", columns="name", values="close")
        vol_frame = res_frame.pivot(index="trade_date", columns="name", values="vol")
        rows, cols = colse_frame.shape
        for col in np.arange(cols):
            for row in np.arange(rows - 3, rows - 1):
                if (float(vol_frame.ix[row, col]) / float(vol_frame.ix[row - 1, col]) >= 2 and
                                float(colse_frame.ix[row, col]) / float(colse_frame.ix[row - 1, col]) >= 0.05):
                    print(colse_frame.index[row], colse_frame.columns[col])
                else:
                    pass

if __name__ == '__main__':
	# Stock().stock_basic() # 获取股票基本信息到本地
    # Get_data().stock_pool() # 设置股票池股票
    # Data_init().stock_daily_init() #股票池日线数据更新
    Get_data().stock_vol_match_price() #查询近三日量价配合较好的股票