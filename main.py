from src.analysis.QQZoneAnalysis import QQZoneAnalysis
from src.spider.QQZoneFriendMoodSpider import QQZoneFriendMoodSpider


def test_auto_crawl_and_process():
    # 爬取数据,存到本地redis中
    qqfriend = QQZoneFriendMoodSpider(use_redis=True, debug=False, mood_begin=0, mood_num=-1,
                                      stop_time='2014-06-01',
                                      download_small_image=False, download_big_image=False,
                                      download_mood_detail=True, download_like_detail=False, download_like_names=False,
                                      recover=False)

    qqfriend.get_friend_mood(mood_num=-1, login_method=1)

    # 处理数据，并将结果上传到服务器
    analysis = QQZoneAnalysis(use_redis=True, debug=True,analysis_friend=False, from_web=False)

    for friend in analysis.friend_name_list:
        analysis.change_username(friend['friend_qq'], friend['nick_name'])
        # 重新初始化参数
        analysis.init_parameter()
        analysis.init_file_name()
        # 从redis中读取数据，到mood_details中
        analysis.load_file_from_redis()
        # 清洗数据
        analysis.get_useful_info_from_json()
        # 导出数据到mongodb中
        analysis.export_mood_df(export_csv=False, export_excel=False, export_mongodb=True)


if __name__ == '__main__':
    test_auto_crawl_and_process()
