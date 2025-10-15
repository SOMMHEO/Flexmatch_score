from modules.DB_connection_and_Load_conn_S3_data import *
from modules.data_preprocessing import *
from modules.connected_user_calcuate_flexmatch_score import *


ssh = SSHMySQLConnector()
ssh.load_config_from_json('C:/Users/flexmatch/Desktop/ssom/code/4.Flexmatch_score/config/accounts.json') 
ssh.connect()

load_dotenv()
# aws_access_key = os.getenv("aws_accessKey")
# aws_secret_key = os.getenv("aws_secretKey")

# client = boto3.client(
#     's3',
#     aws_access_key_id=aws_access_key,
#     aws_secret_access_key=aws_secret_key,
#     region_name='ap-northeast-2'
# )


def main():
    # DB data loading
    sales_info, seller_interest_info, conn_user_main_category_info = get_all_infos()

    ## DB instagram data loading
    # bucket_name = 'flexmatch-data'
    table_list = [
        # 'CONN_v2_RECENT_USER_INFO_MTR',
        'S3_CONN_v2_TIME_SERIES_PROFILE_INFO',
        # 'CONN_v2_BY_USER_ID_MEDIA_DTL_INFO',
        'S3_CONN_v2_BY_DATE_MEDIA_AGG_INFO',
        'S3_CONN_v2_PROFILE_INSIGHT_DTL',
        'S3_CONN_v2_MEDIA_INSIGHT_CUM',
        'S3_CONN_v2_PROFILE_INSIGHT_FOLLOWTYPE'
    ]

    # connected_user & not_connected_user common table
    # merged_data_by_table = load_last_weekly_instagram_data(bucket_name, table_list, target_filename='merged_data.parquet')
    merged_data_by_table = get_weekly_instagram_data_from_db(table_list)
    
    # recent_user_info_mtr = merged_data_by_table['CONN_v2_RECENT_USER_INFO_MTR']['day_before_last_week']
    # recent_user_info_mtr = merged_data_by_table['CONN_v2_RECENT_USER_INFO_MTR']['day_before_last_week']
    time_series_profile_info = merged_data_by_table['S3_CONN_v2_TIME_SERIES_PROFILE_INFO']['day_before_last_week']
    # by_user_id_media_dtl_info = merged_data_by_table['S3_CONN_v2_BY_USER_ID_MEDIA_DTL_INFO']['day_before_last_week']
    # by_date_media_agg_info = merged_data_by_table['S3_CONN_v2_BY_DATE_MEDIA_AGG_INFO']['day_before_last_week']

    # recent_user_info_mtr_2 = merged_data_by_table['S3_CONN_v2_RECENT_USER_INFO_MTR']['last_week']
    time_series_profile_info_2 = merged_data_by_table['S3_CONN_v2_TIME_SERIES_PROFILE_INFO']['last_week']
    # by_user_id_media_dtl_info_2 = merged_data_by_table['S3_CONN_v2_BY_USER_ID_MEDIA_DTL_INFO']['last_week']
    # by_date_media_agg_info_2 = merged_data_by_table['S3_CONN_v2_BY_DATE_MEDIA_AGG_INFO']['last_week']

    conn_profile_insight = merged_data_by_table['S3_CONN_v2_PROFILE_INSIGHT_DTL']['day_before_last_week']
    conn_profile_insight_2 = merged_data_by_table['S3_CONN_v2_PROFILE_INSIGHT_DTL']['last_week']

    conn_media_insight = merged_data_by_table['S3_CONN_v2_MEDIA_INSIGHT_CUM']['day_before_last_week']
    conn_media_insight_2 = merged_data_by_table['S3_CONN_v2_MEDIA_INSIGHT_CUM']['last_week']

    conn_profile_insight_followtype = merged_data_by_table['S3_CONN_v2_PROFILE_INSIGHT_FOLLOWTYPE']['day_before_last_week']
    conn_profile_insight_followtype_2 = merged_data_by_table['S3_CONN_v2_PROFILE_INSIGHT_FOLLOWTYPE']['last_week']
    
    
    ## Data preprocessing
    
    # ------- connected_user_data --------
    # c_unique_user = recent_user_info_mtr_2[recent_user_info_mtr_2['acnt_conn_yn']=='Y']['acnt_id'].to_list()
    # c_recent_user_info_mtr_2 = recent_user_info_mtr_2[recent_user_info_mtr_2['acnt_id'].isin(c_unique_user)]

    # c_time_series_profile_info = time_series_profile_info[time_series_profile_info['acnt_id'].isin(c_unique_user)]
    # c_time_series_profile_info_2 = time_series_profile_info_2[time_series_profile_info_2['acnt_id'].isin(c_unique_user)]

    # # by_user_id_media_dtl_info = by_user_id_media_dtl_info[by_user_id_media_dtl_info['acnt_id'].isin(c_unique_user)]
    # c_by_user_id_media_dtl_info_2 = by_user_id_media_dtl_info_2[by_user_id_media_dtl_info_2['acnt_id'].isin(c_unique_user)]

    # c_conn_profile_insight = conn_profile_insight[conn_profile_insight['acnt_id'].isin(c_unique_user)]
    # c_conn_profile_insight_2 = conn_profile_insight_2[conn_profile_insight_2['acnt_id'].isin(c_unique_user)]

    # c_conn_profile_insight_followtype = conn_profile_insight_followtype[conn_profile_insight_followtype['acnt_id'].isin(c_unique_user)]
    # c_conn_profile_insight_followtype_2 = conn_profile_insight_followtype_2[conn_profile_insight_followtype_2['acnt_id'].isin(c_unique_user)]

    # # connected_user 같은 경우에는 conn_media_insight 안에 있는 게 media_agg랑 동일하기 때문에 해당 부분을 쓰면 당장은 문제가 없음
    # unique_media = c_by_user_id_media_dtl_info_2['media_id'].unique()
    # # c_by_date_media_agg_info_2 = by_date_media_agg_info_2[by_date_media_agg_info_2['media_id'].isin(unique_media)]
    # c_conn_media_insight_2 = conn_media_insight_2[conn_media_insight_2['media_id'].isin(unique_media)]
        
    # user_info, timeseries, timeseries_2, user_followtype, user_followtype_2, media_info, media_agg, all_merged_df, media_engagement_merged_df, media_engagement_profile_merged_df, time_series_merged_df = conn_create_merged_df(
    #                                                                                                                     c_recent_user_info_mtr_2,
    #                                                                                                                     c_time_series_profile_info,
    #                                                                                                                     c_time_series_profile_info_2,
    #                                                                                                                     c_by_user_id_media_dtl_info_2,
    #                                                                                                                     c_conn_media_insight_2,
    #                                                                                                                     c_conn_profile_insight_followtype,
    #                                                                                                                     c_conn_profile_insight_followtype_2)
    
    # user_info.loc[:,'influencer_scale_type'] = user_info.apply(influencer_scale_type, axis=1)

    ### 수정 버전
    all_table_list = [
        time_series_profile_info,
        time_series_profile_info_2,

        conn_profile_insight,
        conn_profile_insight_2,

        conn_media_insight,
        conn_media_insight_2,

        conn_profile_insight_followtype,
        conn_profile_insight_followtype_2
        ]

    for df in all_table_list:
        if 'acnt_id' in df.columns:
            df['acnt_id'] = df['acnt_id'].astype(str)

    # 기준 acnt_id 값은 recent_user_info_mtr이었지만, media_insight로 대체
    c_unique_user = conn_media_insight_2['acnt_id'].to_list()
    c_conn_profile_insight_2 = conn_profile_insight_2[conn_profile_insight_2['acnt_id'].isin(c_unique_user)]

    # by_user_id_media_dtl_info = by_user_id_media_dtl_info[by_user_id_media_dtl_info['acnt_id'].isin(c_unique_user)]
    # c_by_user_id_media_dtl_info_2 = by_user_id_media_dtl_info_2[by_user_id_media_dtl_info_2['acnt_id'].isin(c_unique_user)]

    c_time_series_profile_info = time_series_profile_info[time_series_profile_info['acnt_id'].isin(c_unique_user)]
    c_time_series_profile_info_2 = time_series_profile_info_2[time_series_profile_info_2['acnt_id'].isin(c_unique_user)]

    c_conn_profile_insight_followtype = conn_profile_insight_followtype[conn_profile_insight_followtype['acnt_id'].isin(c_unique_user)]
    c_conn_profile_insight_followtype_2 = conn_profile_insight_followtype_2[conn_profile_insight_followtype_2['acnt_id'].isin(c_unique_user)]

    c_conn_media_insight = conn_media_insight[conn_media_insight['acnt_id'].isin(c_unique_user)]
    c_conn_media_insight_2 = conn_media_insight_2[conn_media_insight_2['acnt_id'].isin(c_unique_user)]

    # connected_user 같은 경우에는 conn_media_insight 안에 있는 게 media_agg랑 동일하기 때문에 해당 부분을 쓰면 당장은 문제가 없음
    # media_dtl_info는 누적이 안되고 있는 문제가 있어서 media_insgith로 대체
    unique_media = conn_media_insight_2['media_id'].unique()
    # c_by_date_media_agg_info_2 = by_date_media_agg_info_2[by_date_media_agg_info_2['media_id'].isin(unique_media)]
    c_conn_media_insight_2 = conn_media_insight_2[conn_media_insight_2['media_id'].isin(unique_media)]
    
    user_info, timeseries, timeseries_2, user_followtype, user_followtype_2, media_insight_info, all_merged_df, media_engagement_profile_merged_df, time_series_merged_df = create_merged_df(
                                                                                                                        c_conn_profile_insight_2,
                                                                                                                        c_time_series_profile_info,
                                                                                                                        c_time_series_profile_info_2,
                                                                                                                        c_conn_media_insight_2,
                                                                                                                        c_conn_profile_insight_followtype,
                                                                                                                        c_conn_profile_insight_followtype_2)
    all_merged_df.loc[:,'influencer_scale_type'] = all_merged_df.apply(influencer_scale_type, axis=1)

    ## calculate flexmatch score - non_connected_user
    activity_df = calculate_activity_score(media_insight_info)
    check_inf(activity_df)

    growth_rate_df = calculate_follower_growth_rate(timeseries, timeseries_2)

    # follower_engagment_df = calculate_follower_engagement(media_engagement_profile_merged_df)
    # check_inf(follower_engagment_df)

    follower_loyalty_df = calculate_follower_loyalty(conn_profile_insight_followtype_2, timeseries_2)
    check_inf(follower_loyalty_df)

    post_efficiency_df = calculate_post_efficiency_df(all_merged_df)
    check_inf(post_efficiency_df)
    
    post_popularity_df = calculate_post_popularity_df(media_engagement_profile_merged_df)
    check_inf(post_popularity_df)

    ad_efficiency_df = calculate_ad_efficiency(conn_user_main_category_info, sales_info, post_efficiency_df)
    check_inf(ad_efficiency_df)

    ## create flexmatch score table by influencer scale type
    connected_flexmatch_score_table = connected_user_flexmatch_score(all_merged_df, activity_df, growth_rate_df, follower_loyalty_df, post_efficiency_df, post_popularity_df, ad_efficiency_df)
    
    seller_interest_info['ig_user_id'] = seller_interest_info['ig_user_id'].replace('', np.nan, regex=True)
    conn_user = seller_interest_info[(seller_interest_info['ig_user_id'].notnull()) & (seller_interest_info['ig_user_id'] != '')]
    conn_user_interestcategory = conn_user[['ig_user_id', 'interestcategory']].rename({'ig_user_id' : 'acnt_id'}, axis=1)
    
    # interest category 
    connected_flexmatch_score_table = pd.merge(connected_flexmatch_score_table, conn_user_interestcategory, on='acnt_id')

    connected_flexmatch_score_table['interestcategory'] = connected_flexmatch_score_table['interestcategory'].fillna('뷰티')
    connected_flexmatch_score_table['interestcategory'] = connected_flexmatch_score_table['interestcategory'].apply(
            lambda x: '뷰티' if pd.isna(x) or (isinstance(x, str) and x.strip() == '') else x)
    
    category_map = {  
            'BABY/KIDS': '베이비/키즈',
            'BEAUTY': '뷰티',
            'FASHION': '패션',
            'FOOD': '푸드',
            'HEALTHY': '헬시',
            'HOME/LIVING': '홈/리빙',
            'SERVICE': '서비스',
            'SPORT': '스포츠',
            'TEST 카테고리.. TEST': '뷰티'     
        }  

    for k, v in category_map.items():
        connected_flexmatch_score_table['interestcategory'] = connected_flexmatch_score_table['interestcategory'].str.replace(k, v)

    # score table에 main category merge
    conn_user_main_category_info = conn_user_main_category_info[['acnt_id', 'main_category', 'top_3_category']]
    conn_user_main_category_info['acnt_id'] = conn_user_main_category_info['acnt_id'].astype(str)
    connected_flexmatch_score_table = pd.merge(connected_flexmatch_score_table, conn_user_main_category_info, on='acnt_id', how='inner')
    # final preprocessing after table merge    
    connected_flexmatch_score_table = connected_flexmatch_score_table.drop_duplicates(subset=['acnt_id', 'acnt_nm'])
    
    nano = connected_flexmatch_score_table[connected_flexmatch_score_table['influencer_scale_type']=='nano']
    micro = connected_flexmatch_score_table[connected_flexmatch_score_table['influencer_scale_type']=='micro']
    mid = connected_flexmatch_score_table[connected_flexmatch_score_table['influencer_scale_type']=='mid']
    macro = connected_flexmatch_score_table[connected_flexmatch_score_table['influencer_scale_type']=='macro']
    mega = connected_flexmatch_score_table[connected_flexmatch_score_table['influencer_scale_type']=='mega']

    # connected_user 추가

    influencer_scale_names=['nano', 'micro', 'mid', 'macro', 'mega']
    influencer_scale_df_list=[nano, micro, mid, macro, mega] # 여기에 connected user도 같이 포함하면 한번에 업로드 되지 않을까 함

    normalized_df, normalized_all_dic = normalize_influencer_scores(influencer_scale_names, influencer_scale_df_list)
    
    ## DB Insert
    ssh = SSHMySQLConnector()
    ssh.load_config_from_json('C:/Users/flexmatch/Desktop/ssom/code/4.Flexmatch_score/config/accounts.json') 
    ssh.connect(True)
    ssh.insert_query_with_lookup('op_mem_seller_score', list(normalized_all_dic.values()))


if __name__=='__main__':
    main()