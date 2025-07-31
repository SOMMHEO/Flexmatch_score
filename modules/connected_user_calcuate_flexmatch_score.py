import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sklearn.preprocessing import MinMaxScaler
from functools import reduce

# inf 있는지 없는지 확인
def check_inf(df):
    float_cols = df.select_dtypes(include=['float64', 'float32']).columns

    mask_inf = np.isinf(df[float_cols]).any(axis=1)
    mask_neginf = np.isneginf(df[float_cols]).any(axis=1)

    invalid_rows = df[mask_inf | mask_neginf]

    print(f"⚠️ inf / -inf 포함 행 개수: {len(invalid_rows)}개")
    # display(invalid_rows)

def calculate_activity_score(recent_media_dtl_df): # 두 개의 테이블 중 가장 최근
    media_dtl_copy = recent_media_dtl_df.copy()
    media_dtl_copy = media_dtl_copy.drop_duplicates(subset=['acnt_id', 'media_id', 'media_cn'])
    media_dtl_copy['reg_dt'] = pd.to_datetime(media_dtl_copy['reg_dt'])
    media_dtl_copy = media_dtl_copy.sort_values(['acnt_id', 'reg_dt'])

    # 게시물 간격 계산
    media_dtl_copy['prev_reg_dt'] = media_dtl_copy.groupby('acnt_id')['reg_dt'].shift(1)
    media_dtl_copy['gap_days'] = (media_dtl_copy['reg_dt'] - media_dtl_copy['prev_reg_dt']).dt.days

    # gap_days가 NaN인 첫 번째 포스트 제외 후 평균 간격 계산
    activity_df = media_dtl_copy.dropna(subset=['gap_days']).groupby('acnt_id')['gap_days'].mean().reset_index()
    activity_df.rename(columns={'gap_days': 'avg_upload_interval'}, inplace=True)

    # 활동성 점수 계산 (간격의 역수로 환산) -> 점수 정규화 (업로드 간격이 짧을수록 점수가 높아지도록 역수를 취해서 계산한 것)
    # 업로드 간격이 너무 짧은 유저의 경우 inf로 계산되는 것을 방지하기 위해서 scaling 진행
    activity_df['avg_upload_interval'] = activity_df['avg_upload_interval'].replace(0, 0.1)
    activity_df['activity_score'] =  (1 / activity_df['avg_upload_interval']) * 100
    
    return activity_df

def calculate_follower_growth_rate(time_series_df, recent_time_series_df):
    time_series_df.loc[:, 'acnt_id'] = time_series_df['acnt_id'].astype(object)
    recent_time_series_df.loc[:, 'acnt_id'] = recent_time_series_df['acnt_id'].astype(object)

    influencer_list = time_series_df['acnt_id'].unique()
    recent_time_series_df = recent_time_series_df[recent_time_series_df['acnt_id'].isin(influencer_list)]
    time_series_merged_df = pd.merge(time_series_df, recent_time_series_df, on='acnt_id')

    time_series_merged_df['follow_growth_rate'] = ((time_series_merged_df['follower_cnt_y'] - time_series_merged_df['follower_cnt_x']) / (time_series_merged_df['follower_cnt_x'])) * 100
    growth_rate_df = time_series_merged_df[['acnt_id', 'follow_growth_rate']]

    return growth_rate_df

# def calculate_follower_engagement(media_engagement_profile_merged_df):
#     media_engagement_profile_merged_df_copy = media_engagement_profile_merged_df[['acnt_id', 'media_id', 'follower_cnt', 'like_cnt', 'cmnt_cnt', 'share_cnt', 'save_cnt', 'views_cnt', 'reach_cnt', 'media_cnt']]
#     # media_id는 조회가 되지만 실제로 media_cnt는 없는 경우가 있음
#     # media_engagement_profile_merged_df_copy = media_engagement_profile_merged_df_copy[media_engagement_profile_merged_df_copy['media_cnt'] != 0]
    
#     engaged_df = media_engagement_profile_merged_df_copy.groupby(['acnt_id']).agg({
#         'like_cnt' : 'sum',
#         'cmnt_cnt' : 'sum',
#         'share_cnt' : 'sum',
#         'save_cnt' : 'sum',
#         'media_cnt': 'first',
#         'follower_cnt' : 'first',
#     }).reset_index()

#     engaged_df['estimated_total_engagement'] = ((engaged_df['like_cnt'] + engaged_df['cmnt_cnt'] + engaged_df['share_cnt'] + engaged_df['save_cnt']) / ( engaged_df['media_cnt']*engaged_df['follower_cnt']))
#     engaged_df['follower_total_engagement'] = engaged_df['estimated_total_engagement'] * 100
    
#     follower_engagment_df = engaged_df

#     return follower_engagment_df

# def calculate_follower_loyalty(time_series_merged_df):
#     time_series_merged_df_copy = time_series_merged_df[['acnt_id', 'follower_cnt_x', 'follower_cnt_y']].copy()

#     time_series_merged_df_copy.loc[:, 'follower_change'] = (time_series_merged_df_copy['follower_cnt_y'] - time_series_merged_df_copy['follower_cnt_x'])

#     def estimate_new_follower(row):
#         if row['follower_change'] < 0:
#             return 0
#         else:
#             return row['follower_change']

#     time_series_merged_df_copy.loc[:, 'new_follower'] = time_series_merged_df_copy.apply(estimate_new_follower, axis=1)
#     time_series_merged_df_copy.loc[:, 'unfollowed'] = time_series_merged_df_copy['follower_cnt_x'] + time_series_merged_df_copy['new_follower'] - time_series_merged_df_copy['follower_cnt_y']
#     time_series_merged_df_copy.loc[:, 'follower_retention_rate'] = ((time_series_merged_df_copy['follower_cnt_x'] - time_series_merged_df_copy['unfollowed']) / time_series_merged_df_copy['follower_cnt_x']) * 100
#     time_series_merged_df_copy.loc[:, 'follower_retention_rate'] = time_series_merged_df_copy['follower_retention_rate'].round(2)

#     follower_loyalty_df = time_series_merged_df_copy

#     return follower_loyalty_df

def calculate_follower_loyalty(conn_profile_insight_followtype_2, timeseries_2):
    follow_type_df = conn_profile_insight_followtype_2.pivot_table(
        index=["acnt_id", "base_ymd", "acnt_nm"],  # 고유 식별자 기준으로 고정
        columns="follow_type_nm",  # FOLLOWER / NON_FOLLOWER가 컬럼으로 올라감
        values="follow_unfollow_cnt",  # 우리가 관심있는 값
        fill_value=0  # 값이 없을 경우 0으로 채움
    )

    # preprocessing follow_type_df
    follow_type_df.columns = [f"{col.lower()}_follow_unfollow_cnt" for col in follow_type_df.columns]
    follow_type_df.rename(columns={'follower_follow_unfollow_cnt':'new_follower', 'non_follower_follow_unfollow_cnt':'unfollowed'}, inplace=True)
    follow_type_df = follow_type_df.reset_index()
    follow_type_df

    timeseries_2_copy = timeseries_2[['acnt_id', 'follower_cnt']]
    follow_type_df_copy = follow_type_df.copy()
    follow_type_df_copy = pd.merge(follow_type_df, timeseries_2_copy, on='acnt_id', how='left')
    follow_type_df_copy.dropna(inplace=True)

    follow_type_df_copy['net_gain'] = follow_type_df_copy['new_follower'] - follow_type_df_copy['unfollowed']
    follow_type_df_copy['follower_retention_rate'] = (follow_type_df_copy['net_gain'] / (follow_type_df_copy['follower_cnt'] + follow_type_df_copy['new_follower'])) * 100

    return follow_type_df_copy

def calculate_post_efficiency_df(media_engagement_profile_merged_df):
    media_engagement_profile_merged_df_copy = media_engagement_profile_merged_df.copy()

    media_engagement_profile_merged_df_copy['post_efficiency'] = ((media_engagement_profile_merged_df_copy['like_cnt'] + media_engagement_profile_merged_df_copy['cmnt_cnt'] + media_engagement_profile_merged_df_copy['save_cnt'] + media_engagement_profile_merged_df_copy['share_cnt']) / media_engagement_profile_merged_df_copy['follower_cnt']) * 100
    # media_engagement_profile_merged_df_copy['post_efficiency'] = ((media_engagement_profile_merged_df_copy['like_cnt'] + media_engagement_profile_merged_df_copy['cmnt_cnt'] + media_engagement_profile_merged_df_copy['save_cnt'] + media_engagement_profile_merged_df_copy['share_cnt']) / media_engagement_profile_merged_df_copy['views_cnt']) * 100
    post_efficiency_df = media_engagement_profile_merged_df_copy.groupby('acnt_id')['post_efficiency'].mean().reset_index()
    post_efficiency_df.rename(columns={'post_efficiency': 'avg_post_efficiency'}, inplace=True)

    return post_efficiency_df

def calculate_post_popularity_df(media_engagement_profile_merged_df):
    media_engagement_profile_merged_df_copy = media_engagement_profile_merged_df.copy()

    media_engagement_profile_merged_df_copy['post_popularity'] = ((media_engagement_profile_merged_df_copy['like_cnt'] + media_engagement_profile_merged_df_copy['cmnt_cnt'] + media_engagement_profile_merged_df_copy['save_cnt'] + media_engagement_profile_merged_df_copy['share_cnt']) / media_engagement_profile_merged_df_copy['views_cnt']) * 100
    # check_inf(media_engagement_profile_merged_df_copy)
    media_engagement_profile_merged_df_copy.replace([np.inf, -np.inf], np.nan, inplace=True)
    media_engagement_profile_merged_df_copy.dropna(subset=['post_popularity'], inplace=True)

    post_popularity_df = media_engagement_profile_merged_df_copy.groupby('acnt_id')['post_popularity'].mean().reset_index()
    post_popularity_df.rename(columns={'post_popularity': 'avg_post_popularity'}, inplace=True)

    return post_popularity_df

def connected_user_flexmatch_score(user_info, activity_df, growth_rate_df, follower_loyalty_df, post_efficiency_df, post_popularity_df):
    # 크리에이터 활동성
    creator_activity_score = activity_df[['acnt_id', 'activity_score']]
    # 트렌드지수 (팔로워 순변화량)
    creator_follow_growth_rate = growth_rate_df[['acnt_id', 'follow_growth_rate']] # db 변수명 수정
    # 팔로워 참여도
    # follower_engagement = follower_engagement_df[['acnt_id', 'follower_total_engagement']]
    # 팔로워 충성도
    follower_loyalty = follower_loyalty_df[['acnt_id', 'follower_retention_rate']]
    # 콘텐츠 효율성
    post_efficiency = post_efficiency_df[['acnt_id', 'avg_post_efficiency']]
    # 콘텐츠 인기도
    post_popularity = post_popularity_df[['acnt_id', 'avg_post_popularity']]

    # data_list
    df_list = [creator_activity_score, creator_follow_growth_rate, follower_loyalty, post_efficiency, post_popularity]

    from functools import reduce

    flexmatch_score = reduce(lambda left, right: pd.merge(left, right, on='acnt_id', how='left'), df_list)
    user_info_nm = user_info[['acnt_id', 'acnt_nm', 'influencer_scale_type']]
    flexmatch_score = pd.merge(flexmatch_score, user_info_nm, on='acnt_id')
    flexmatch_score = flexmatch_score[['acnt_id', 'acnt_nm', 'influencer_scale_type', 'activity_score', 'follow_growth_rate', 'follower_retention_rate', 'avg_post_efficiency', 'avg_post_popularity']]

    connected_flexmatch_score_table = flexmatch_score.copy()
    connected_flexmatch_score_table.dropna(inplace=True)
    
    return connected_flexmatch_score_table


# def normalize_influencer_scores(
#     influencer_scale_names, 
#     influencer_scale_df_list, 
#     reverse_columns=None, 
#     log_columns=None, 
#     feature_range=(1, 5)
# ):
#     if reverse_columns is None:
#         reverse_columns = []
#     if log_columns is None:
#         log_columns = []

#     normalized_df_dict = {}

#     for name, df in zip(influencer_scale_names, influencer_scale_df_list):
#         cleaned = df.copy()

#         # 무한대 및 NaN 제거
#         float_cols = cleaned.select_dtypes(include='float64').columns
#         cleaned[float_cols] = cleaned[float_cols].replace([np.inf, -np.inf], np.nan)
#         cleaned = cleaned.dropna(subset=float_cols)

#         if cleaned.empty:
#             continue

#         norm_df = pd.DataFrame(index=cleaned.index)
#         for col in float_cols:
#             col_data = cleaned[col]
#             if col in log_columns:
#                 col_data = np.log1p(col_data)
#             scaler = MinMaxScaler(feature_range=feature_range)
#             norm_col = scaler.fit_transform(col_data.values.reshape(-1, 1))
#             if col in reverse_columns:
#                 norm_df[col] = feature_range[1] - norm_col.ravel()
#             else:
#                 norm_df[col] = norm_col.ravel()

#         norm_df['acnt_id'] = cleaned['acnt_id'].values
#         norm_df['acnt_nm'] = cleaned['acnt_nm'].values
#         norm_df['influencer_scale_type'] = name

#         normalized_df_dict[name] = norm_df

#     normalized_all_df = pd.concat(normalized_df_dict.values(), ignore_index=True)
#     normalized_all_dic = normalized_all_df.to_dict(orient='index')

#     return normalized_all_df, normalized_all_dic

def normalize_influencer_scores(
    influencer_scale_names, 
    influencer_scale_df_list, 
    reverse_columns=None, 
    log_columns=None, 
    feature_range=(1, 5)
):
    if reverse_columns is None:
        reverse_columns = []
    if log_columns is None:
        log_columns = []

    normalized_df_list = []

    for name, df in zip(influencer_scale_names, influencer_scale_df_list):
        cleaned = df.copy()

        # 무한대 및 NaN 제거
        float_cols = cleaned.select_dtypes(include='float64').columns
        cleaned[float_cols] = cleaned[float_cols].replace([np.inf, -np.inf], np.nan)
        cleaned = cleaned.dropna(subset=float_cols)

        if cleaned.empty:
            continue

        # 관심 카테고리 첫 번째 값 추출
        if 'interestcategory' in cleaned.columns:
            cleaned['main_interest_category'] = cleaned['interestcategory'].apply(
                lambda x: str(x).split('@')[0] if pd.notnull(x) else '뷰티'
            )
        else:
            cleaned['main_interest_category'] = '뷰티'

        # (관심카테고리, 스케일타입) 조합별 그룹화
        cleaned['influencer_scale_type'] = name  # 확실히 넣어두기
        grouped = cleaned.groupby(['main_interest_category', 'influencer_scale_type'])

        for (category, scale_type), group in grouped:
            norm_df = pd.DataFrame(index=group.index)

            # 그룹 내 인원 수 체크
            if len(group) == 1:
                # 1명인 경우 모든 점수를 1.0으로 고정
                for col in float_cols:
                    norm_df.loc[group.index, col] = 3.0
            else:
                # 여러명인 경우 정규화 진행
                for col in float_cols:
                    col_data = group[col]
                    if col in log_columns:
                        col_data = np.log1p(col_data)

                    max_val = col_data.max()
                    min_val = col_data.min()

                    if max_val == min_val:
                        # 모든 값이 동일하면 1.0 고정
                        norm_df.loc[group.index, col] = 3.0
                    else:
                        scaler = MinMaxScaler(feature_range=feature_range)
                        norm_col = scaler.fit_transform(col_data.values.reshape(-1, 1))
                        if col in reverse_columns:
                            norm_df[col] = feature_range[1] - norm_col.ravel()
                        else:
                            norm_df[col] = norm_col.ravel()

            # 공통 컬럼 복사
            norm_df['acnt_id'] = group['acnt_id'].values
            norm_df['acnt_nm'] = group['acnt_nm'].values
            norm_df['influencer_scale_type'] = scale_type
            norm_df['main_interest_category'] = category

            normalized_df_list.append(norm_df)

    normalized_all_df = pd.concat(normalized_df_list, ignore_index=True)
    normalized_all_dic = normalized_all_df.to_dict(orient='index')

    return normalized_all_df, normalized_all_dic


