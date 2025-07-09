from modules.DB_connection_and_Load_S3 import *
from modules.data_preprocessing import *
from modules.non_connected_user_calcuate_flexmatch_score import *


ssh = SSHMySQLConnector()
ssh.load_config_from_json('C:/Users/ehddl/Desktop/업무/code/config/ssh_db_config.json') 
ssh.connect()

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
    ## s3 data loading
    bucket_name = 'flexmatch-data'
    table_list = ['RECENT_USER_INFO_MTR', 'TIME_SERIES_PROFILE_INFO', 'BY_USER_ID_MEDIA_DTL_INFO', 'BY_DATE_MEDIA_AGG_INFO',
                  'CONN_PROFILE_INSIGHT_DTL', 'CONN_MEDIA_INSIGHT']

    # connected_user & not_connected_user common table
    merged_data_by_table = load_weekly_instagram_data(bucket_name, table_list, weeks_back=2, target_filename='merged_data.parquet')
    
    recent_user_info_mtr = merged_data_by_table['RECNET_USER_INFO_MTR']['prev_week']
    time_series_profile_info = merged_data_by_table['TIME_SERIES_PROFILE_INFO']['prev_week']
    by_user_id_media_dtl_info = merged_data_by_table['BY_USER_ID_MEDIA_DTL_INFO']['prev_week']
    by_date_media_agg_info = merged_data_by_table['BY_DATE_MEDIA_AGG_INFO']['prev_week']

    recent_user_info_mtr_2 = merged_data_by_table['RECNET_USER_INFO_MTR']['current_week']
    time_series_profile_info_2 = merged_data_by_table['TIME_SERIES_PROFILE_INFO']['current_week']
    by_user_id_media_dtl_info_2 = merged_data_by_table['BY_USER_ID_MEDIA_DTL_INFO']['current_week']
    by_date_media_agg_info_2 = merged_data_by_table['BY_DATE_MEDIA_AGG_INFO']['current_week']

    # only conntected user table
    conn_profile_insight = merged_data_by_table['CONN_PROFILE_INSIGHT_DTL']['prev_week']
    conn_profile_insight_2 = merged_data_by_table['CONN_PROFILE_INSIGHT_DTL']['current_week']
    
    conn_media_insight = merged_data_by_table['CONN_MEDIA_INSIGHT']['prev_week']
    conn_media_insight_2 = merged_data_by_table['CONN_MEDIA_INSIGHT']['current_week']

    ## Data preprocessing
    # -------- not_connected_user data -------
    
    # unique_user = recent_user_info_mtr['acnt_id'].unique()
    nc_unique_user = recent_user_info_mtr_2[recent_user_info_mtr_2['connacnt_conn_yn']=='N']['acnt_id'].to_list()
    
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
    
    # ------- connected_user_data --------
    # connected_unique_user
    # nc_unique_user를 이용하면 각 주차별로 새로 추가된 not_connected_user가 추가될 수 있음

    c_unique_user = recent_user_info_mtr_2[recent_user_info_mtr_2['connacnt_conn_yn']=='Y']['acnt_id'].to_list()

    c_recent_user_info_mtr_2 = recent_user_info_mtr_2[recent_user_info_mtr_2['acnt_id'].isin(c_unique_user)]

    c_time_series_profile_info = time_series_profile_info[time_series_profile_info['acnt_id'].isin(c_unique_user)]
    c_time_series_profile_info_2 = time_series_profile_info_2[time_series_profile_info_2['acnt_id'].isin(c_unique_user)]
    
    c_by_user_id_media_dtl_info_2 = by_user_id_media_dtl_info_2[by_user_id_media_dtl_info_2['acnt_id'].isin(c_unique_user)]

    c_unique_media = c_by_user_id_media_dtl_info_2['media_id'].unique()
    c_by_date_media_agg_info_2 = by_date_media_agg_info_2[by_date_media_agg_info_2['media_id'].isin(c_unique_media)]

    c_recent_user_info_mtr_2.loc[:,'influencer_scale_type'] = nc_recent_user_info_mtr_2.apply(influencer_scale_type, axis=1)

    c_user_info, c_timeseries, c_timeseries_2, c_media_info, c_media_agg, c_all_merged_df, c_media_engagement_merged_df, c_media_engagement_profile_merged_df, c_media_aggtime_series_merged_df = create_merged_df(
                                                                                                                                                                            c_recent_user_info_mtr_2,
                                                                                                                                                                            c_time_series_profile_info,
                                                                                                                                                                            c_time_series_profile_info_2,
                                                                                                                                                                            c_by_user_id_media_dtl_info_2,
                                                                                                                                                                            c_by_date_media_agg_info_2)
    
    ## calculate flexmatch score - non_connected_user
    activity_df = calculate_activity_score(nc_media_info)
    check_inf(activity_df)

    growth_rate_df = calculate_follower_growth_rate(nc_timeseries, nc_timeseries_2)

    follower_engagment_df = calculate_follower_engagement(nc_media_engagement_profile_merged_df)
    check_inf(follower_engagment_df)

    follower_loyalty_df = calculate_follower_loyalty(nc_time_series_merged_df)
    check_inf(follower_loyalty_df)

    post_efficiency_df = calculate_post_efficiency_df(nc_media_engagement_profile_merged_df)
    check_inf(post_efficiency_df)

    ## create flexmatch score table by influencer scale type
    not_connected_flexmatch_score_table = not_connected_user_flexmatch_score(activity_df, growth_rate_df, follower_engagment_df, follower_loyalty_df, post_efficiency_df)
    
    nc_nano = not_connected_flexmatch_score_table[not_connected_flexmatch_score_table['influencer_scale_type']=='nano']
    nc_micro = not_connected_flexmatch_score_table[not_connected_flexmatch_score_table['influencer_scale_type']=='micro']
    nc_mid = not_connected_flexmatch_score_table[not_connected_flexmatch_score_table['influencer_scale_type']=='mid']
    nc_macro = not_connected_flexmatch_score_table[not_connected_flexmatch_score_table['influencer_scale_type']=='macro']
    nc_mega = not_connected_flexmatch_score_table[not_connected_flexmatch_score_table['influencer_scale_type']=='mega']

    # connected_user 추가

    influencer_scale_names=['nano', 'micro', 'mid', 'macro', 'mega']
    influencer_scale_df_list=[nc_nano, nc_micro, nc_mid, nc_macro, nc_mega] # 여기에 connected user도 같이 포함하면 한번에 업로드 되지 않을까 함

    normalized_df, normalized_dic = normalize_influencer_scores(influencer_scale_names, influencer_scale_df_list)
    
    ## DB Insert
    ssh.insert_query_with_lookup('op_mem_seller_score', normalized_dic)


if __name__=='__main__':
    main()