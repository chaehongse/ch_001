import os
import pandas as pd
import pymysql
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import warnings
import re

warnings.simplefilter("ignore", UserWarning)

# 설정
folder_path = r'D:\python\stock\data'
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '5434',
    'database': 'stock_db',
    'charset': 'utf8mb4'
}
table_name = 'stock_data'

# 스타일 오류 복구
def fix_excel_file(file_path):
    try:
        pd.read_excel(file_path, engine='openpyxl')
        return file_path
    except Exception:
        print(f"{os.path.basename(file_path)}: 스타일 문제로 복구 중...")

        fixed_path = file_path.replace('.xlsx', '_fixed.xlsx')
        df_all = pd.read_excel(file_path, header=None)

        header = df_all.iloc[0].tolist()
        data = df_all.iloc[1:].reset_index(drop=True)

        wb = Workbook()
        ws = wb.active
        ws.append(header)

        for r in dataframe_to_rows(data, index=False, header=False):
            ws.append(r)

        wb.save(fixed_path)
        print(f"{os.path.basename(fixed_path)}로 복구 완료")
        return fixed_path

# 테이블 자동 생성
def create_table_if_not_exists(df, conn):
    cursor = conn.cursor()

    # 컬럼명 정제
    columns = df.columns.tolist()
    clean_columns = [re.sub(r'\W+', '_', str(col)).strip('_') for col in columns]

    column_defs = [f"`{col}` VARCHAR(255)" for col in clean_columns]

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        id INT AUTO_INCREMENT PRIMARY KEY,
        {', '.join(column_defs)}
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    cursor.execute(create_sql)
    conn.commit()
    cursor.close()

    # 컬럼명 정제 적용
    df.columns = clean_columns
    return df

# 데이터 업로드
def upload_to_mysql(file_path):
    fixed_path = fix_excel_file(file_path)
    df = pd.read_excel(fixed_path, header=0)
    df = df.iloc[0:].reset_index(drop=True)  # 2번째 행부터 데이터

    df = df.where(pd.notnull(df), None)  # NaN → None

    # DB 연결
    conn = pymysql.connect(**db_config)

    # 테이블이 없다면 생성
    df = create_table_if_not_exists(df, conn)

    cursor = conn.cursor()

    columns = df.columns.tolist()
    placeholders = ','.join(['%s'] * len(columns))
    insert_sql = f"""
        INSERT INTO `{table_name}` ({','.join([f'`{col}`' for col in columns])})
        VALUES ({placeholders})
    """

    for _, row in df.iterrows():
        cursor.execute(insert_sql, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"{os.path.basename(file_path)} → DB 업로드 완료")

# 모든 엑셀 파일 처리
def process_all_files():
    for file in os.listdir(folder_path):
        if file.endswith('.xlsx') and not file.endswith('_fixed.xlsx'):
            file_path = os.path.join(folder_path, file)
            upload_to_mysql(file_path)

# 실행
if __name__ == '__main__':
    process_all_files()
