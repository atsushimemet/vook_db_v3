import json
import re
from datetime import datetime

import pandas as pd
import requests
from google.cloud import bigquery
from google.oauth2 import service_account
from logzero import logger

from config.local_config import CLIENT_RAKUTEN


def main():
    RUN_DATE = datetime.today().strftime("%Y-%m-%d")

    keyword = "リーバイス ヴィンテージ パンツ デニム"
    REQ_URL = "https://app.rakuten.co.jp/services/api/IchibaItem/Search/20170706"
    HITS_PER_PAGE = 30  # NOTE: ページあたり商品数

    params = {
        "applicationId": CLIENT_RAKUTEN["APPLICATION_ID"],
        "affiliateId": CLIENT_RAKUTEN["AFF_ID"],
        "format": "json",
        "formatVersion": "2",
        "sort": "-itemPrice",
        "minPrice": 100,
        "keyword": keyword,
        "hits": HITS_PER_PAGE,
    }

    # 1-1: 楽天の商品検索APIにリクエストを投げ、レスポンスを受け取る。
    cnt = 1
    params["page"] = cnt
    res = requests.get(REQ_URL, params)

    # 1-2: リクエストが成功していなければ、その旨をログに吐く、そうでなければ、、その旨をログに吐き、レスポンスから情報を抽出する。
    res_cd = res.status_code
    if res_cd != 200:
        logger.warning("Error")
    else:
        logger.info("Sucess")
    res = json.loads(res.text)

    # 1-3: 必要な項目を抽出する。
    NECESSARY_KEYS = [
        "itemCode",
        "itemName",
        "itemPrice",
        "itemCaption",
        "affiliateUrl",
        "mediumImageUrls",
    ]
    tmp_df = pd.DataFrame(res["Items"])[NECESSARY_KEYS]
    tmp_df["runDate"] = RUN_DATE
    KEYS_ORDER = [
        "runDate",
        "itemCode",
        "itemName",
        "itemCaption",
        "itemPrice",
        "affiliateUrl",
        "mediumImageUrls",
    ]
    KEYS_RENAMED = [
        "run_date",
        "item_code",
        "item_name",
        "item_caption",
        "item_price",
        "affiliate_url",
        "medium_image_urls",
    ]
    tmp_df = (
        tmp_df[KEYS_ORDER].rename(columns=dict(zip(KEYS_ORDER, KEYS_RENAMED))).copy()
    )

    df_medium_image_urls = pd.DataFrame(
        [urls for urls in tmp_df["medium_image_urls"]],
        columns=[
            "medium_image_urls_1",
            "medium_image_urls_2",
            "medium_image_urls_3",
        ],
    )
    df_medium_image_urls["item_code"] = tmp_df["item_code"]
    pd.merge()

    # 日時は明示的にdatetimeにしないと、to_gbq()の内部でintにしようとして、下記のエラーになる
    # pyarrow.lib.ArrowTypeError: object of type <class 'str'> cannot be converted to int
    tmp_df["run_date"] = pd.to_datetime(tmp_df["run_date"])
    # medium_image_urlsがリスト、文字列に変換する。
    # ArrowTypeError: Expected bytes, got a 'list' object
    tmp_df["medium_image_urls"] = [repr(urls) for urls in tmp_df["medium_image_urls"]]
    assert type(tmp_df["medium_image_urls"].iloc[0]) == str

    # 1-3-0-1: ローデータのアップロード
    # 認証情報の取得
    credentials = service_account.Credentials.from_service_account_file(
        # "./vook-v1-fd0e7a9c1cfd.json",
        "vook-v1-427cd425d99d.json",
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    # clientの定義
    project_id = "vook-v1"
    client = bigquery.Client(project=project_id, credentials=credentials)
    # スキーマの定義
    # Schemaを定義
    # https://webservice.rakuten.co.jp/documentation/ichiba-item-search#outputParameter
    schema = [
        bigquery.SchemaField("run_date", "DATE", mode="REQUIRED", description="API実行日"),
        bigquery.SchemaField(
            "item_code", "STRING", mode="REQUIRED", description="商品コード"
        ),
        bigquery.SchemaField("item_name", "STRING", mode="REQUIRED", description="商品名"),
        bigquery.SchemaField(
            "item_caption", "STRING", mode="REQUIRED", description="商品説明文"
        ),
        bigquery.SchemaField(
            "item_price", "INTEGER", mode="REQUIRED", description="商品価格"
        ),
        bigquery.SchemaField(
            "affiliate_url", "STRING", mode="REQUIRED", description="アフィリエイトURL"
        ),
        bigquery.SchemaField(
            "medium_image_urls", "STRING", mode="REQUIRED", description="商品画像128x128URL"
        ),
    ]

    # テーブル名を決める
    table_name = "rakuten_product_search_api"
    dataset_id = "raw"
    table_id = "{}.{}.{}".format(client.project, dataset_id, table_name)

    # スキーマは上で定義したものを利用
    table = bigquery.Table(table_id, schema=schema)

    # 分割テーブルの設定(ここではORDER_DT)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field="run_date"
    )
    # テーブル作成を実行
    table = client.create_table(table, exists_ok=True)  # WARNINGS

    # pd.DataFrameデータをテーブルに格納する
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig(
        schema=schema,
    )
    job = client.load_table_from_dataframe(
        tmp_df,
        table_ref.table_id,
        job_config=job_config,
        location="US",
    )

    job.result()

    # 1-3-1: 画像のリサイズ
    ORIGINAL_SIZE = "128"
    size = "400"
    tmp_df["medium_image_urls"] = [
        [url.replace(ORIGINAL_SIZE, size) for url in urls]
        for urls in tmp_df["medium_image_urls"]
    ]

    assert tmp_df.shape[0] <= 3000

    # 1-5: キーワードからアイテムカテゴリ1、アイテムカテゴリ2を抽出する。
    item_category_1 = keyword.split(" ")[2]

    # 1-6: アイテムカテゴリ1に合致しないレコードを除外する。
    tmp_df_item_category_1 = (
        tmp_df[[True if item_category_1 in v else False for v in tmp_df["item_name"]]]
        .reset_index(drop=True)
        .copy()
    )

    # 1-7: ブランドをtmp_df_item_category_1の列に定義する。
    tmp_df_item_category_1["brand"] = keyword.split(" ")[0]
    # 1-8: アイテムカテゴリ1をtmp_df_item_category_1の列に定義する。
    tmp_df_item_category_1["item_1"] = keyword.split(" ")[2]
    # 1-9: アイテムカテゴリ2をtmp_df_item_category_1の列に定義する。
    tmp_df_item_category_1["item_2"] = keyword.split(" ")[3]

    # 1-10: 年代をitem_nameから作成し、tmp_df_item_category_1の列に定義する。
    has_ages_str = [
        re.findall(r"\d{2}年代|\d{2}s|\d{2}'s", v)
        for v in tmp_df_item_category_1["item_name"]
    ]
    has_ages_int = [
        [
            int(age.replace("年代", "").replace("'s", "").replace("s", ""))
            if age
            else age
            for age in ages
        ]
        for ages in has_ages_str
    ]
    has_ages_int_not_66 = [[age for age in ages if age != 66] for ages in has_ages_int]
    age_str = []
    for age in has_ages_int_not_66:
        if len(set(age)) == 0:
            age_str.append(None)
        elif len(set(age)) == 1:
            age_str.append(f"{age[0]}")
        elif len(set(age)) == 2:
            age_str.append(f"{min(age)}~{max(age)}")
        else:
            raise Exception("想定外の値が検知されました")
    tmp_df_item_category_1["age"] = age_str
    none_rate = sum([True if not age else False for age in age_str]) / len(age_str)
    print(f"none_rate:{none_rate}")

    # 1-11: モデルをtmp_df_item_category_1の列に定義する。
    l_model = []
    for v in tmp_df_item_category_1["item_name"]:
        if "大戦モデル" in v:
            l_model.append("大戦モデル")
        elif "XX" in v:
            l_model.append("XX")
        elif re.search(r"ビッグE|ビッグ E|BigE|Big E|BIGE|BIG E", v):
            l_model.append("Big E")
        elif "66前期" in v:
            l_model.append("66前期")
        elif "66後期" in v:
            l_model.append("66後期")
        elif "赤耳" in v:
            l_model.append("赤耳")
        else:
            l_model.append(None)

    # 1-12: サイズをitem_captionから作成し、tmp_df_item_category_1の列に定義する。
    # 1-12-1: インチ表記を抽出する。
    inch = [
        int(
            re.search(
                r"[wWwW][2-40-9][2-40-9]|実寸[2-40-9][2-40-9]|サイズ[2-40-9][2-40-9]", v
            )
            .group()
            .replace("w", "")
            .replace("W", "")
            .replace("w", "")
            .replace("W", "")
            .replace("サイズ", "")
            .replace("実寸", "")
        )
        if re.search(
            r"[wWwW][2-40-9][2-40-9]|実寸[2-40-9][2-40-9]|サイズ[2-40-9][2-40-9]", v
        )
        else None
        for v in tmp_df_item_category_1["item_caption"]
    ]
    none_rate = sum([True if not v else False for v in inch]) / len(inch)
    print(f"none_rate:{none_rate}")

    # 1-12-2: cm表記を抽出する。
    cm = [
        int(
            re.search(
                r"ウエスト[7-9][0-9]cm|ウエスト.[7-9][0-9]cm|ウエスト[7-9][0-9]cm|ウエスト.[7-9][0-9]cm",
                v,
            )
            .group()
            .replace("ウエスト", "")
            .replace("cm", "")
            .replace("cm", "")
            .replace("約", "")
            .replace(" ", "")
            .replace("：", "")
            .replace(":", "")
        )
        if re.search(r"ウエスト[7-9][0-9]cm|ウエスト.[7-9][0-9]cm", v)
        else None
        for v in tmp_df_item_category_1["item_caption"]
    ]
    CONVERTER = 2.54
    cm2inch = [int(round(cm / CONVERTER)) if cm else cm for cm in cm]

    # 1-12-3: インチ表記がないものはcmからインチに変換したものを使用する。
    inch_last = []
    for inch_, cm2inch_ in zip(inch, cm2inch):
        if inch_:
            inch_last.append(inch_)
        else:
            inch_last.append(cm2inch_)
    tmp_df_item_category_1["size"] = inch_last
    none_rate = sum([True if not v else False for v in inch_last]) / len(inch_last)
    print(f"none_rate:{none_rate}")

    # 1-13: 価格をdf_item_category_1の列に定義する。既にされている。
    # df_item_category_1["itemPrice"]

    # Issue: ディテールが取得できる項目

    # 2-1: 出来上がったテーブルを日次パーティションテーブルとして、bigqueryにアップロードする。
    tmp_df_item_category_1["medium_image_urls"]


if __name__ == "__main__":
    main()
