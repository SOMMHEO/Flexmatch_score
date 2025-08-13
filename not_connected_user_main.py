from modules.DB_connection_and_Load_not_conn_S3_data import *
from modules.data_preprocessing import *
from modules.not_connected_user_calcuate_flexmatch_score import *


load_dotenv()
aws_access_key = os.getenv("aws_accessKey")
aws_secret_key = os.getenv("aws_secretKey")

client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name='ap-northeast-2'
)

def main():
    ## DB data loading
    sales_info, seller_interest_info, not_conn_user_main_category_info = get_all_infos()

    ## s3 data loading
    bucket_name = 'flexmatch-data'
    table_list = ['RECENT_USER_INFO_MTR', 'TIME_SERIES_PROFILE_INFO', 'BY_USER_ID_MEDIA_DTL_INFO', 'BY_DATE_MEDIA_AGG_INFO']
    # table_list = ['EXTERNAL_RECENT_USER_INFO_MTR', 'EXTERNAL_TIME_SERIES_PROFILE_INFO', 'EXTERNAL_BY_USER_ID_MEDIA_DTL_INFO', 'EXTERNAL_BY_DATE_MEDIA_AGG_INFO']

    # connected_user & not_connected_user common table
    merged_data_by_table = load_weekly_instagram_data(bucket_name, table_list, weeks_back=2, target_filename='merged_data.parquet')
    
    recent_user_info_mtr = merged_data_by_table['RECENT_USER_INFO_MTR']['prev_week']
    time_series_profile_info = merged_data_by_table['TIME_SERIES_PROFILE_INFO']['prev_week']
    by_user_id_media_dtl_info = merged_data_by_table['BY_USER_ID_MEDIA_DTL_INFO']['prev_week']
    by_date_media_agg_info = merged_data_by_table['BY_DATE_MEDIA_AGG_INFO']['prev_week']

    recent_user_info_mtr_2 = merged_data_by_table['RECENT_USER_INFO_MTR']['current_week']
    time_series_profile_info_2 = merged_data_by_table['TIME_SERIES_PROFILE_INFO']['current_week']
    by_user_id_media_dtl_info_2 = merged_data_by_table['BY_USER_ID_MEDIA_DTL_INFO']['current_week']
    by_date_media_agg_info_2 = merged_data_by_table['BY_DATE_MEDIA_AGG_INFO']['current_week']

    # recent_user_info_mtr = merged_data_by_table['EXTERNAL_RECENT_USER_INFO_MTR']['prev_week']
    # time_series_profile_info = merged_data_by_table['EXTERNAL_TIME_SERIES_PROFILE_INFO']['prev_week']
    # by_user_id_media_dtl_info = merged_data_by_table['EXTERNAL_BY_USER_ID_MEDIA_DTL_INFO']['prev_week']
    # by_date_media_agg_info = merged_data_by_table['EXTERNAL_BY_DATE_MEDIA_AGG_INFO']['prev_week']

    # recent_user_info_mtr_2 = merged_data_by_table['EXTERNAL_RECENT_USER_INFO_MTR']['current_week']
    # time_series_profile_info_2 = merged_data_by_table['EXTERNAL_TIME_SERIES_PROFILE_INFO']['current_week']
    # by_user_id_media_dtl_info_2 = merged_data_by_table['EXTERNAL_BY_USER_ID_MEDIA_DTL_INFO']['current_week']
    # by_date_media_agg_info_2 = merged_data_by_table['EXTERNAL_BY_DATE_MEDIA_AGG_INFO']['current_week']


    # 혹시 몰라서 일단 한번 적용
    recent_user_info_mtr['acnt_id'] = recent_user_info_mtr['acnt_id'].astype(str)
    time_series_profile_info['acnt_id'] = time_series_profile_info['acnt_id'].astype(str)
    recent_user_info_mtr_2['acnt_id'] = recent_user_info_mtr_2['acnt_id'].astype(str)
    time_series_profile_info_2['acnt_id'] = time_series_profile_info_2['acnt_id'].astype(str)

    by_user_id_media_dtl_info_2['acnt_id'] = by_user_id_media_dtl_info_2['acnt_id'].astype(str)
    by_date_media_agg_info_2['media_id'] = by_date_media_agg_info_2['media_id'].astype(str)

    ## Data preprocessing
    # -------- not_connected_user data -------
    
    # unique_user = recent_user_info_mtr['acnt_id'].unique()
    nc_unique_user = recent_user_info_mtr_2[recent_user_info_mtr_2['acnt_conn_yn']=='N']['acnt_id'].to_list()
    
    nc_recent_user_info_mtr_2 = recent_user_info_mtr_2[recent_user_info_mtr_2['acnt_id'].isin(nc_unique_user)]

    nc_time_series_profile_info = time_series_profile_info[time_series_profile_info['acnt_id'].isin(nc_unique_user)]
    nc_time_series_profile_info_2 = time_series_profile_info_2[time_series_profile_info_2['acnt_id'].isin(nc_unique_user)]
    
    # nc_by_user_id_media_dtl_info = by_user_id_media_dtl_info[by_user_id_media_dtl_info['acnt_id'].isin(nc_unique_user)]
    nc_by_user_id_media_dtl_info_2 = by_user_id_media_dtl_info_2[by_user_id_media_dtl_info_2['acnt_id'].isin(nc_unique_user)]

    nc_unique_media = nc_by_user_id_media_dtl_info_2['media_id'].unique()
    nc_by_date_media_agg_info_2 = by_date_media_agg_info_2[by_date_media_agg_info_2['media_id'].isin(nc_unique_media)]

    # influencer scale type
    # nc_recent_user_info_mtr.loc[:, 'influencer_scale_type'] = nc_recent_user_info_mtr.apply(influencer_scale_type, axis=1)
    nc_recent_user_info_mtr_2.loc[:,'influencer_scale_type'] = nc_recent_user_info_mtr_2.apply(influencer_scale_type, axis=1)

    nc_user_info, nc_timeseries, nc_timeseries_2, nc_media_info, nc_media_agg, nc_all_merged_df, nc_media_engagement_merged_df, nc_media_engagement_profile_merged_df, nc_time_series_merged_df = create_merged_df(
                                                                                                                                                                            nc_recent_user_info_mtr_2,
                                                                                                                                                                            nc_time_series_profile_info,
                                                                                                                                                                            nc_time_series_profile_info_2,
                                                                                                                                                                            nc_by_user_id_media_dtl_info_2,
                                                                                                                                                                            nc_by_date_media_agg_info_2)
    
   
    ## calculate flexmatch score - non_connected_user
    activity_df = calculate_activity_score(nc_media_info)
    check_inf(activity_df)

    growth_rate_df = calculate_follower_growth_rate(nc_timeseries, nc_timeseries_2)

    # follower_engagment_df = calculate_follower_engagement(nc_media_engagement_profile_merged_df)
    # check_inf(follower_engagment_df)

    follower_loyalty_df = calculate_follower_loyalty(nc_time_series_merged_df)
    check_inf(follower_loyalty_df)

    post_efficiency_df = calculate_post_efficiency_df(nc_media_engagement_profile_merged_df)
    check_inf(post_efficiency_df)

    ## create flexmatch score table by influencer scale type
    not_connected_flexmatch_score_table = not_connected_user_flexmatch_score(nc_user_info, activity_df, growth_rate_df, follower_loyalty_df, post_efficiency_df)
    
    conn_list = seller_interest_info[(seller_interest_info['ig_user_id'].notnull()) & (seller_interest_info['ig_user_id'] != '')]['ig_user_id'].to_list()
    not_conn_user = seller_interest_info[~seller_interest_info['ig_user_id'].isin(conn_list)]
    not_conn_user = not_conn_user[['add1', 'interestcategory']]

    not_conn_user['acnt_nm'] = not_conn_user['add1'].apply(clean_acnt_nm)

    ## score table에 interest category merge
    # external의 경우 해당 부분은 제외
    not_connected_flexmatch_score_table = pd.merge(not_connected_flexmatch_score_table, not_conn_user, on='acnt_nm')
    not_connected_flexmatch_score_table['interestcategory'] = not_connected_flexmatch_score_table['interestcategory'].fillna('뷰티')
    not_connected_flexmatch_score_table['interestcategory'] = not_connected_flexmatch_score_table['interestcategory'].apply(
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
        not_connected_flexmatch_score_table['interestcategory'] = not_connected_flexmatch_score_table['interestcategory'].str.replace(k, v)

    print(not_connected_flexmatch_score_table['acnt_id'].nunique())

    # score table에 main category merge
    not_conn_user_main_category_info = not_conn_user_main_category_info[~not_conn_user_main_category_info['acnt_id'].isin(conn_list)]
    not_conn_user_main_category_info = not_conn_user_main_category_info[['acnt_id', 'main_category', 'top_3_category']]
    not_conn_user_main_category_info['acnt_id'] = not_conn_user_main_category_info['acnt_id'].astype(str)

    not_connected_flexmatch_score_table_2 = pd.merge(not_connected_flexmatch_score_table, not_conn_user_main_category_info, on='acnt_id', how='left')
    print(not_connected_flexmatch_score_table_2['acnt_id'].nunique())

    # final preprocessing after table merge
    not_connected_flexmatch_score_table_2 = not_connected_flexmatch_score_table_2.drop_duplicates(subset=['acnt_id', 'acnt_nm'])
    
    nc_nano = not_connected_flexmatch_score_table_2[not_connected_flexmatch_score_table_2['influencer_scale_type']=='nano']
    nc_micro = not_connected_flexmatch_score_table_2[not_connected_flexmatch_score_table_2['influencer_scale_type']=='micro']
    nc_mid = not_connected_flexmatch_score_table_2[not_connected_flexmatch_score_table_2['influencer_scale_type']=='mid']
    nc_macro = not_connected_flexmatch_score_table_2[not_connected_flexmatch_score_table_2['influencer_scale_type']=='macro']
    nc_mega = not_connected_flexmatch_score_table_2[not_connected_flexmatch_score_table_2['influencer_scale_type']=='mega']

    # connected_user 추가
    influencer_scale_names=['nano', 'micro', 'mid', 'macro', 'mega']
    influencer_scale_df_list=[nc_nano, nc_micro, nc_mid, nc_macro, nc_mega] # 여기에 connected user도 같이 포함하면 한번에 업로드 되지 않을까 함

    normalized_df, normalized_all_dic = normalize_influencer_scores(influencer_scale_names, influencer_scale_df_list)
    
    ## DB Insert
    ssh = SSHMySQLConnector()
    ssh.load_config_from_json('C:/Users/ehddl/Desktop/업무/code/config/ssh_db_config.json') 
    ssh.connect(True)
    ssh.insert_query_with_lookup('op_mem_seller_score', list(normalized_all_dic.values()))


if __name__=='__main__':
    main()