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

def conn_create_merged_df(user_info_df, timeseries_df, timeseries_df_2, media_info_df, media_insight_df, user_followtype_df, user_followtype_df_2): # media_agg, profile_insight X
    # merge 시에 같은 이름의 열이 두개여서 error 발생하기 때문에 insight에서는 삭제
    media_insight = media_insight_df.drop(['acnt_id'], axis=1)
    media_engagement_merged_df = pd.merge(media_info_df, media_insight, on='media_id', how='outer')
    # print(len(media_engagement_merged_df['acnt_id'].unique()))

    ### 방법 1
    # 단 한개의 게시물이라도 like가 비공개인 influencer 제거 & media_cnt_가 0인 사람도 제외
    # by_user_na_like_count = media_engagement_merged_df[media_engagement_merged_df['like_cnt'].isna()].groupby(['acnt_id'])['media_id'].count()
    # na_like_user = by_user_na_like_count[by_user_na_like_count > 0].index
    # no_media_user = user_info_df[user_info_df['media_cnt'] == 0]['acnt_id'].to_list()
    # except_user = list(na_like_user) + no_media_user
    # media_engagement_merged_df = media_engagement_merged_df[~media_engagement_merged_df['acnt_id'].isin(except_user)].reset_index()

    ### 방법 2
    # 미디어가 한 개도 없는 유저 제거 & 게시물의 like가 비공개인 경우에는 그걸 제외한 게시물의 좋아요 평균으로 채워넣기
    # 근데 여기서 like만 비공인건지 나머지 값들도 비공이 되는건지 확인을 하긴 해야함
    # mean 값 자체가 nan인 사람들도 제외
    # 근데 여기서 프로페셔널 전환은 이미 되어있지만, views라는 지표 자체가 나중에 나와서 전부 0으로 찍히는 경우가 있음. -> 어떻게 해결?
    no_media_user = user_info_df[user_info_df['media_cnt'] == 0]['acnt_id'].to_list()
    media_engagement_merged_df = media_engagement_merged_df[~media_engagement_merged_df['acnt_id'].isin(no_media_user)].reset_index()

    media_engagement_merged_groupby_df = media_engagement_merged_df.groupby('acnt_id')[['like_cnt', 'cmnt_cnt', 'share_cnt', 'save_cnt', 'views_cnt', 'reach_cnt']].mean()
    media_engagement_merged_groupby_df = np.ceil(media_engagement_merged_groupby_df)
    fillna_user = media_engagement_merged_groupby_df[media_engagement_merged_groupby_df['like_cnt'] > 1].index

    media_engagement_merged_df = media_engagement_merged_df[media_engagement_merged_df['acnt_id'].isin(fillna_user)].reset_index()

    engagement_cols = ['like_cnt', 'cmnt_cnt', 'share_cnt', 'save_cnt', 'views_cnt', 'reach_cnt']
    for col in engagement_cols:
        media_engagement_merged_df[col] = media_engagement_merged_df.apply(
        lambda row: media_engagement_merged_groupby_df.at[row['acnt_id'], col] if pd.isna(row[col]) else row[col], axis=1)

    user_list = media_engagement_merged_df['acnt_id'].unique()
    media_list = media_engagement_merged_df['media_id'].unique()

    # merge하면서 제거된 리스트가 있기 때문에, 해당 부분 다시 삭제 후에 새로운 merge 파일 생성
    user_info = user_info_df[user_info_df['acnt_id'].isin(user_list)]
    timeseries = timeseries_df[timeseries_df['acnt_id'].isin(user_list)]
    timeseries_2 = timeseries_df_2[timeseries_df_2['acnt_id'].isin(user_list)]
    media_info = media_info_df[media_info_df['acnt_id'].isin(user_list)]
    
    user_followtype = user_followtype_df[user_followtype_df['acnt_id'].isin(user_list)]
    user_followtype_2 = user_followtype_df_2[user_followtype_df_2['acnt_id'].isin(user_list)]
    
    media_insight_info = media_insight[media_insight['media_id'].isin(media_list)]

    all_merged_df_a = pd.merge(user_info, timeseries, on='acnt_id')
    all_merged_df_b = pd.merge(all_merged_df_a, media_info, on='acnt_id')
    all_merged_df = pd.merge(all_merged_df_b, media_insight_info, on='media_id')
    
    media_engagement_profile_merged_df = pd.merge(media_engagement_merged_df, user_info_df, on='acnt_id')
    time_series_merged_df = pd.merge(timeseries, timeseries_df_2, on='acnt_id')

    return user_info, timeseries, timeseries_2, user_followtype, user_followtype_2, media_info, media_insight_info, all_merged_df, media_engagement_merged_df, media_engagement_profile_merged_df, time_series_merged_df