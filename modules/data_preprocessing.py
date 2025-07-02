import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sklearn.preprocessing import MinMaxScaler


def influencer_scale_type(row):
    count = row['follower_cnt']
    if count < 1000:
        return 'nano'
    elif 1000 <= count <= 10000:
        return 'micro'
    elif 10000 < count <= 100000:
        return 'mid'
    elif 100000 < count <= 500000:
        return 'macro'
    else:
        return 'mega'
    
def create_merged_df(user_info_df, timeseries_df, timeseries_df_2, media_info_df, media_agg_df):
    media_engagement_merged_df = pd.merge(media_info_df, media_agg_df, on='media_id', how='outer')
    # print(len(media_engagement_merged_df['acnt_id'].unique()))

    # 단 한개의 게시물이라도 like가 비공개인 influencer 제거
    by_user_na_like_count = media_engagement_merged_df[media_engagement_merged_df['like_cnt'].isna()].groupby(['acnt_id'])['media_id'].count()
    na_like_user = by_user_na_like_count[by_user_na_like_count > 0].index
    # print(len(na_like_user))
    media_engagement_merged_df = media_engagement_merged_df[~media_engagement_merged_df['acnt_id'].isin(na_like_user)].reset_index()

    user_list = media_engagement_merged_df['acnt_id'].unique()
    # print(len(user_list))
    media_list = media_engagement_merged_df['media_id'].unique()

    # merge하면서 제거된 리스트가 있기 때문에, 해당 부분 다시 삭제 후에 새로운 merge 파일 생성
    user_info = user_info_df[user_info_df['acnt_id'].isin(user_list)]
    timeseries = timeseries_df[timeseries_df['acnt_id'].isin(user_list)]
    timeseries_2 = timeseries_df_2[timeseries_df_2['acnt_id'].isin(user_list)]
    media_info = media_info_df[media_info_df['acnt_id'].isin(user_list)]
    media_agg = media_agg_df[media_agg_df['media_id'].isin(media_list)]

    all_merged_df_a = pd.merge(user_info, timeseries, on='acnt_id')
    all_merged_df_b = pd.merge(all_merged_df_a, media_info, on='acnt_id')
    all_merged_df = pd.merge(all_merged_df_b, media_agg, on='media_id')
    
    media_engagement_profile_merged_df = pd.merge(media_engagement_merged_df, user_info_df, on='acnt_id')
    time_series_merged_df = pd.merge(timeseries, timeseries_df_2, on='acnt_id')

    return user_info, timeseries, timeseries_2, media_info, media_agg, all_merged_df, media_engagement_merged_df, media_engagement_profile_merged_df, time_series_merged_df