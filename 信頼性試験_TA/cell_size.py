import os
import datetime
from datetime import date,timedelta
import unicodedata
import pathlib
from pathlib import Path
import glob
import re
import cx_Oracle as oracle
import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine
import openpyxl



# SQLテンプレート読み込み用
def read_sql_template(path, encoding='UTF-8'):
    # ファイルを開く
    f = open(path, 'r', encoding=encoding)
    
    # 一行ずつ読み込んで結合（ただし、コメント行は結合しない）
    sqlcmd = ' '.join([s.strip() for s in f.readlines() if not '--' in s])
    return sqlcmd



# DBのコネクションオブジェクトを返す
def load_from_db(dist="vuser"):

    # 読み込みにはsqlalchemyを使ってオラクルに接続(cx_Oracleだとwarningがでるのでsqlamchemyに変更)
    DIALECT = 'oracle'
    SQL_DRIVER = 'cx_oracle'
    
    if dist == "vuser":
        USERNAME = 'VUSER' #enter your username
        PASSWORD = 'v123' #enter your password
        HOST = '10.60.28.37' #enter the oracle db host url
        SERVICE = 'psh3dbv' # enter the oracle db service name
    elif dist == "tabuser":
        USERNAME = 'tabuser' #enter your username
        PASSWORD = 'tab123' #enter your password
        HOST = '10.60.28.21' #enter the oracle db host url
        SERVICE = 'psh1dbv' # enter the oracle db service name
    
    PORT = 1521 # enter the oracle port number
    
    ENGINE_PATH_WIN_AUTH = DIALECT + '+' + SQL_DRIVER + '://' + USERNAME + ':' + PASSWORD +'@' + HOST + ':' + str(PORT) + '/?service_name=' + SERVICE

    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    
    return engine



#CX oracleでMESサーバーへ接続（接続情報以外は定型）
class conn_MES_LWR:
    def __init__(self, host= "10.60.28.21", port="1521", service="psh1dbv",
                       scheme="tabuser",username="tabuser",password="tab123"):        
        self.host = host
        self.port = port
        self.service  = service

        self.scheme   = scheme
        self.username = username
        self.password = password
    
    def __enter__(self):
        
        # tns:Oracleが命名したDB接続用インターフェース技術の名前
        
        # インターフェイスオブジェクトの作成
        self.tns  = oracle.makedsn(self.host, self.port, service_name=self.service) if self.host else None
        # 接続を確立
        self.conn = oracle.connect(self.username, self.password, self.tns) if self.tns else None
        # カーソルの取得
        self.curs = self.conn.cursor() if self.conn else None
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        if self.curs is not None: self.curs.close()
        if self.conn is not None: self.conn.close()
        
        

def upload_data(insert_sql=None, df=None):
    
    # DBに格納できるようにデータフレームを二次元配列に変換
    rows = [list(x) for x in df.values]
    
    with conn_MES_LWR() as mesdb:
        # executemany()で複数行のデータを一括でインサート
        mesdb.curs.executemany(insert_sql, rows)
        mesdb.conn.commit()
        
        

# 検査ロット文字列から頭５文字を取り出す関数
def get_first_5_char(klot):
    return klot[:5]

# 検査ロット文字列から検査ライン番号を取り出す関数
def get_lineno(klot):
    return klot[7]



def make_df_cell_size(can_csv_file_list=None, df_conn=None):
    
    # 各csvファイルに含まれる寸法データをDFに格納して一つにまとめる
    df_for_each_csv_file = []
    for csv_file_name in can_csv_file_list:

        # 試験装置から出力されるcsvファイルを読み込む
        csv_file_path = csv_file_name
        df_cell_size = pd.read_csv(csv_file_path, encoding='shift-jis')

        # 必要な行のみに絞る('測定時刻'がNaNの行は不要)
        df_cell_size.dropna(subset = ['測定時刻'], inplace=True)

        #NaN -> 空文字に変換(NaNのままだとDBにインサートできない)
        df_cell_size.fillna("", inplace=True)

        # DB用に必要な列のみに絞る
        df_cell_size_col_filtered = df_cell_size[["測定時刻", "判定", "シリアル　カウンタ", "[1]底面　3ｍｍ", "[2]天面　3mm"]]

        # リストに各DFを追加
        df_for_each_csv_file.append(df_cell_size_col_filtered)

    
    df_cell_size_joined = pd.concat(df_for_each_csv_file, ignore_index=True)
    
    # 判定列が'OK'のセルのみDBにアップロードするのでフィルタリング
    df_cell_size_joined_ok = df_cell_size_joined[df_cell_size_joined['判定'] == 'OK']
    
    # 全てのセルID(シリアルカウンタ)のリストを作成
    cellid_list = df_cell_size_joined_ok["シリアル　カウンタ"].to_list()
    
    # 各セルIDのデータをDBから取得してDFに読み込み、リスト化
    cell_df_list = []
    which_table = []
    for cellid in cellid_list:

        # SQLファイルからSQLを読み込む
        # H1_VD300_KS_SIテーブル用のSQL
        sqlcmd_for_get_klot_line1 = read_sql_template('./get_klot_from_line1.sql', encoding='shift-jis')
        # H2_VD300_KS_SIテーブル用のSQL
        sqlcmd_for_get_klot_line2 = read_sql_template('./get_klot_from_line2.sql', encoding='shift-jis')

        # 読み込んだSQLのセルIDを対象のセルIDに置換
        sqlcmd_for_get_klot_line1 = sqlcmd_for_get_klot_line1.replace("cell_id", cellid)
        sqlcmd_for_get_klot_line2 = sqlcmd_for_get_klot_line2.replace("cell_id", cellid)


        # まずH1_VD300_KS_SIで検索
        df = pd.read_sql(sqlcmd_for_get_klot_line1, df_conn)

        # H1_VD300_KS_SIで見つからなければH2_VD300_KS_SIに検索をかけにいく
        get_from_line2 = None
        if len(df) == 0:
            # どっちのテーブルからデータを取得したかを識別するためのフラグ
            get_from_line2 = True
            df = pd.read_sql_query(sqlcmd_for_get_klot_line2, df_conn)


        # 各セルIDのDFをリストにまとめる
        cell_df_list.append(df)

    df_cell_joined = pd.concat(cell_df_list, ignore_index=True)
    
    df_cell_size_joined_ok["Klot"] = df_cell_joined["検査ロットＮＯ"]
    df_cell_size_joined_ok["Klot5Char"] = df_cell_joined["検査ロットＮＯ"].apply(get_first_5_char)
    df_cell_size_joined_ok["LineNo"] = df_cell_joined["検査ロットＮＯ"].apply(get_lineno)
    
    
    return df_cell_size_joined_ok



# 抜取セル幅寸法テーブル用SQL
insert_cell_size = 'INSERT INTO "TABUSER"."ReliabilityTest(SmpSize)26TA" ("MeasuringTime", "Klot", "Klot5Char", "LineNo", "CellId", "CellSize(Top)", "CellSize(Bottom)") VALUES (:1,:2,:3,:4,:5,:6,:7)'



# Excel書き込み用の関数
def write_list_2d(sheet, l_2d, start_row, start_col):
    for y, row in enumerate(l_2d):
        for x, cell in enumerate(row):
            sheet.cell(row=start_row + y,
                       column=start_col + x,

                       value=l_2d[y][x])
            
    

def output_to_excel(df_to_excel=None):
    
    # カレントディレクトリを取得
    cwd = os.getcwd()
    
    # 結果出力先のExcelファイル名を取得
    for f in glob.glob(cwd + "\\*"):
        file_name = os.path.split(f)[1]
        if re.findall("result_cell_size", file_name):
            excel_path = file_name
    
    
    # 最終的に出力するDFを二次元配列に変換
    l_2d = df_to_excel.values.tolist()
    
    # Excelファイルの読み込み
    wb = openpyxl.load_workbook(excel_path)
    # ワークシートの読み込み
    ws = wb['Sheet1']

    # データ追加行を設定
    result_df = pd.read_excel("result_cell_size.xlsx", sheet_name="Sheet1")
    result_df = result_df.where(result_df.notna(), None)
    
    max_row = len([line_no for line_no in result_df["検査ロット"] if isinstance(line_no, str)])
    
    insert_row_num = max_row + 2
    
    write_list_2d(ws, l_2d, insert_row_num, 1)

    # Excelファイルを保存
    wb.save(excel_path)
    
    # Excelを閉じる
    wb.close()
    
    


# 対象検査ロットのセル寸法承認申請処理用の関数
def output_cell_size_to_excel():
    
    # 今日の日付データを取得(年・月・日)
    year = datetime.date.today().year
    month = datetime.date.today().month
    day = datetime.date.today().day
    
    # 開始日、終了日の設定
    fmt = "%Y-%m-%d"
    eddt = (datetime.datetime(year, month, day)).strftime(fmt) 
    stdt = (datetime.datetime(year, month, day) + datetime.timedelta(days=-60)).strftime(fmt)
    
    
    # 担当者の入力を受け付け
    pic = input("担当者名を入力してください。: ")

    # 検査ロットの入力を受け付け
    z = True

    klot_input = input("検査ロットを入力してください。(半角): ")
    zenkaku_hankaku = [unicodedata.east_asian_width(char) for char in klot_input]

    while z:
        for zh in zenkaku_hankaku:
            if zh in ["F", "W", "A"]:
                klot_input = input("全角文字が含まれています。半角で再入力してください。: ")
                zenkaku_hankaku = [unicodedata.east_asian_width(char) for char in klot_input]
            else:
                z = False


    # 検査号機の入力を受け付け
    while True:
        line_no_input = input("検査号機を入力してください。(半角): ")
        if line_no_input not in ["1", "2"]:
            line_no_input = input("検査号機の入力に誤りがあります。再入力してください。(半角)")
        else:
            break
            
    # SQLファイルからSQLを読み込む
    while True:
        if line_no_input == '1':
            # H1_VD300_KS_SIテーブル用のSQL
            sqlcmd_for_three_klot = read_sql_template('./get_three_klot_from_line1.sql')
            break
        elif line_no_input == '2':
            # H2_VD300_KS_SIテーブル用のSQL
            sqlcmd_for_three_klot = read_sql_template('./get_three_klot_from_line2.sql')
            break
    
    # 読み込んだSQLの検査ロット・日付の箇所を置換
    sqlcmd_for_three_klot = sqlcmd_for_three_klot.replace("stdt", stdt).replace("eddt", eddt).replace("klot_input", klot_input.upper())
    
    # DB読み込み用の接続を確立
    engine_vuser = load_from_db("vuser")
    
    # 入力された検査ロットを含む前３ロットの検索をかける
    df = pd.read_sql(sqlcmd_for_three_klot, engine_vuser)
    
    # 検査ロットの前５桁を抜き出した列を追加 -> 不要
    # DB抽出時に前５桁のみを取ってくる処理に変更　(2022/05/27 変更)
    #df["Klot5Char"] = df["検査ロットＮＯ"].apply(get_first_5_char)
    
    # 対象３ロットの検査ロット番号をリスト化
    klot_list = df["char5"].to_list()
    
    # 対象の３ロットのセル寸法データをDBから抽出するSQLを読み込む
    sqlcmd_to_get_cell_size = read_sql_template('./get_cell_size.sql')
    
    # 検査号機・検査ロット番号・対象年の変数を実際の値に置き換える
    sqlcmd_to_get_cell_size = sqlcmd_to_get_cell_size.replace("klot1", klot_list[0]).replace("klot2", klot_list[1]).replace("klot3", klot_list[2]).replace("year", str(year)).replace("line_no", line_no_input)
    
    # DB書き込み用の接続を確立
    engine_tabuser = load_from_db("tabuser")
    
    # DBから対象データの読み込み
    df = pd.read_sql(sqlcmd_to_get_cell_size, engine_tabuser)
    
    # '担当者', '判定' 列を追加
    df["PIC"] = pic
    df["judge"] = 'OK'
    
    # Excel出力用に列の並び替え
    df = df.reindex(columns=["MeasuringTime", "Klot5Char", "LineNo", 
                             "CellId", "CellSize(Top)", "CellSize(Bottom)",
                             "PIC", "judge"])
    
    # Excelに結果を書き込み
    output_to_excel(df)
    
    input("Excelへの書き込みが完了しました。任意のキーを押して処理を終了してください。")
    
    
    
# ------------ ↑ ------------- (関数等の定義)

# ------------ ↓ ------------- (実際の処理)



while True:
    mode = input("どの処理を実行しますか？以下から選択してください。\nセル寸法データのアップロード: 1\n対象検査ロットの承認申請: 2\n")
    
    if mode not in ["1", "2"]:
        mode = input("処理内容は、1 or 2 で選択してください。\n")
    else: 
        break
        
        

# mode == "1" -> セル寸法データをDBにアップロード
if mode == "1":
    
    # カレントディレクトリを取得
    cwd = os.getcwd()

    # カレントディレクトリにある"TA承認図_缶測定TABLExxxx.csv"ファイル名を全て取得してリスト化
    can_csv_file_list = []
    for f in glob.glob(cwd + "\\*"):
        csv_file_name = os.path.split(f)[1]
        if re.findall("TA承認図_缶測定TABLE", csv_file_name):
            can_csv_file_list.append(csv_file_name)
            
    
    # DB読み込み用の接続を確立
    engine_vuser = load_from_db("vuser")
    
    
    # セル寸法のDFを作成
    df_cell_size_joined_ok = make_df_cell_size(can_csv_file_list, engine_vuser)
    
    # DBアップロード用に列を並べ替え
    df_cell_size_joined_ok = df_cell_size_joined_ok.reindex(columns=["測定時刻", "Klot", "Klot5Char", "LineNo", "シリアル　カウンタ", "[1]底面　3ｍｍ", "[2]天面　3mm"])
    
    # DBにデータをアップロード
    upload_data(insert_cell_size, df_cell_size_joined_ok)
    
    # DBアップロード後に読み取ったcsvファイルを削除
    for csv_file in can_csv_file_list:
        os.remove(csv_file)


# mode == "2" -> 該当ロットのデータを抽出してExcelに出力
elif mode == "2":
    
    output_cell_size_to_excel()