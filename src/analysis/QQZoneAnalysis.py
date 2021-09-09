import re
import json
import pandas as pd

from src.spider.QQZoneFriendMoodSpider import QQZoneFriendMoodSpider
from src.analysis.Average import Average
from src.util.constant import BASE_DIR, SYSTEM_FONT, EXPIRE_TIME_IN_SECONDS
from src.util.util import get_standard_time_from_mktime2
from pymongo import MongoClient


class QQZoneAnalysis(QQZoneFriendMoodSpider):
    def __init__(self, use_redis=False, debug=False, username='', analysis_friend=False, mood_begin=0, mood_num=-1,
                 stop_time='-1', from_web=False, nickname='', no_delete=True, cookie_text='', pool_flag='127.0.0.1',
                 exprot_excel=True, export_csv=False):
        """

        :param use_redis:
        :param debug:
        :param username:
        :param analysis_friend: 是否要分析好友数据（比如最早的好友、共同好友数量最多的好友），如果要分析该类指标，必须已经获取了好友数据
        :param mood_begin:
        :param mood_num:
        :param stop_time:
        :param from_web:
        :param nickname:
        :param no_delete:
        :param cookie_text:
        :param pool_flag:
        :param exprot_excel:
        :param export_csv:
        """
        QQZoneFriendMoodSpider.__init__(self, use_redis=use_redis, debug=debug, recover=False, username=username,
                                        mood_num=mood_num,
                                        mood_begin=mood_begin, stop_time=stop_time, from_web=from_web,
                                        nickname=nickname,
                                        no_delete=no_delete, cookie_text=cookie_text, analysis=True,
                                        pool_flag=pool_flag)
        self.mood_data = []
        self.mood_data_df = pd.DataFrame()
        self.like_detail_df = []
        self.like_list_names_df = []
        self.analysis_friend = analysis_friend
        self.has_clean_data = False
        self.cmt_df = None
        self.like_uin_df = None
        self.av = Average(use_redis=False, file_name_head=username, analysis=True, debug=debug)
        self.cmt_friend_set = set()
        # 用于绘制词云图的字体，请更改为自己电脑上任意一款支持中文的字体，否则将无法显示中文
        self.system_font = SYSTEM_FONT

    def load_file_from_redis(self):
        self.do_recover_from_exist_data()

    def save_data_to_csv(self):
        pd.DataFrame(self.mood_data_df).to_csv(self.MOOD_DATA_FILE_NAME)

    def save_data_to_excel(self):
        pd.DataFrame(self.mood_data_df).to_excel(self.MOOD_DATA_EXCEL_FILE_NAME)

    def save_data_to_mongodb(self):

        config_path = BASE_DIR + 'config/mongodb_config.json'
        try:
            with open(config_path, 'r', encoding='utf-8') as r:
                mongodb_info = json.load(r)
                # 建立和数据库系统的连接,指定host及port参数
                client = MongoClient(mongodb_info['ip'], mongodb_info['port'])
                # 连接qqmoods数据库,账号密码认证
                db = client.qqmoods
                db.authenticate(mongodb_info['user'], mongodb_info['password'])
                # 连接表
                collection = db[self.username]
                # 插入尾部
                collection.insert_many(self.mood_data)
        except BaseException as e:
            self.format_error(e, "mongodb_config.json does not exist!")
            exit(1)


        # # self.friend_name_list[]
        # for mood in self.mood_data:
        #     qiang.in()

    def load_mood_data(self):
        try:
            self.mood_data_df = pd.read_csv(self.MOOD_DATA_FILE_NAME)
            self.mood_data_df['uin_list'] = self.mood_data_df['uin_list'].apply(
                lambda x: json.loads(x.replace('\'', '\"')))
        except:
            try:
                self.mood_data_df = pd.read_excel(self.MOOD_DATA_EXCEL_FILE_NAME)
                self.mood_data_df['uin_list'] = self.mood_data_df['uin_list'].apply(
                    lambda x: json.loads(x.replace('\'', '\"')))
            except BaseException as e:
                print("加载mood_data_df失败，开始重新清洗数据")
                self.get_useful_info_from_json()

    def get_useful_info_from_json(self):
        """
        从原始动态数据中清洗出有用的信息
        结果存储在self.mood_data_df中
        :return:
        """

        len2 = len(self.mood_details)
        # 解析单条说说详情，并放入内存mood_data
        for i in range(len(self.mood_details)):
            self.parse_mood_detail(self.mood_details[i])
        self.mood_data_df = pd.DataFrame(self.mood_data)

        if self.mood_data_df.empty:
            self.has_clean_data = True
            print("冲洗，输入的数据为空")
            return

        # 按照时间戳降序排序，然后重新以整数作为索引
        self.mood_data_df = self.mood_data_df.sort_values(by='time_stamp', ascending=False).reset_index()
        # 删除某两列，并将原结果替换
        self.mood_data_df.drop('index', axis=1, inplace=True)
        # 删除重复的行
        self.mood_data_df.drop_duplicates('time_stamp', keep='first', inplace=True)

        self.has_clean_data = True

    def calculate_early_send_time(self):
        if self.friend_df is None:
            self.friend_df = pd.read_csv(self.FRIEND_DETAIL_LIST_FILE_NAME)
        if not self.has_clean_data:
            self.get_useful_info_from_json()
        try:
            day_begin_time = self.mood_data_df['time_stamp'].apply(lambda x: get_standard_time_from_mktime2(x))
            day_time_stamp = self.mood_data_df['time_stamp']
            time_diff = day_time_stamp - day_begin_time
            self.mood_data_df['early_time'] = time_diff
            early_mood_time = self.mood_data_df.loc[self.mood_data_df['early_time'] < 60 * 60 * 5]
            has_cmt = False
            if not early_mood_time.empty:
                early_mood_time2 = early_mood_time.loc[early_mood_time['cmt_total_num'] > 1]
                if not early_mood_time2.empty:
                    early_mood_time = early_mood_time2
                    has_cmt = True

            early_mood_time.sort_values(by='early_time', ascending=False, inplace=True)
            earliest_mood = early_mood_time.head(1)
            self.user_info.early_mood_date = earliest_mood['time'].values[0]
            self.user_info.early_mood_time = int(earliest_mood['early_time'].values[0] // (60 * 60))
            self.user_info.early_mood_content = earliest_mood['content'].values[0]
            if has_cmt:
                early_cmt_df = self.av.clean_cmt_df(early_mood_time)
                cmt_friend = early_cmt_df.head(1)
                self.user_info.early_mood_friend = cmt_friend['comment_name'].values[0]
                self.user_info.early_mood_cmt = cmt_friend['comment_content'].values[0]
            if len(self.user_info.early_mood_content) > 20:
                self.user_info.early_mood_content = self.user_info.early_mood_content[:20] + "..."
        except BaseException as e:
            self.format_error(e, "failed to analysis send mood early time")

    def parse_mood_detail(self, mood):
        try:
            msglist = json.loads(mood)
        except BaseException:
            msglist = mood
        tid = msglist['tid']
        try:
            secret = msglist['secret']
            # 过滤私密说说
            if secret:
                pass
            else:
                content = msglist['content']
                time = msglist['createTime']
                time_stamp = msglist['created_time']

                if 'pictotal' in msglist:
                    pic_num = msglist['pictotal']
                else:
                    pic_num = 0
                cmt_num = msglist['cmtnum']
                cmt_list = []
                cmt_total_num = cmt_num
                if 'commentlist' in msglist:
                    comment_list = msglist['commentlist'] if msglist['commentlist'] is not None else []

                    for i in range(len(comment_list)):
                        try:
                            comment = comment_list[i]
                            comment_content = comment['content']
                            if i < 20:
                                comment_name = comment.get('name', 'null')
                                comment_time = comment.get('createTime2', 'null')
                                comment_reply_num = comment.get('replyNum', 0)
                                comment_reply_list = []
                                if comment_reply_num > 0:
                                    for comment_reply in comment['list_3']:
                                        comment_reply_content = comment_reply['content']
                                        # 去掉 @{uin:117557,nick:16,who:1,auto:1} 这种文字
                                        comment_reply_content = \
                                            re.subn(re.compile('\@\{.*?\}'), '', comment_reply_content)[
                                                0].strip()
                                        comment_reply_name = comment_reply.get('name', 'null')
                                        comment_reply_time = comment_reply.get('createTime2', 'null')
                                        comment_reply_list.append(dict(comment_reply_content=comment_reply_content,
                                                                       comment_reply_name=comment_reply_name,
                                                                       comment_reply_time=comment_reply_time))
                            else:
                                comment_poster = comment.get('poster', 'null')
                                if comment_poster != 'null':
                                    comment_name = comment_poster.get('name', 'null')
                                comment_time = comment['postTime']

                                comment_extend = comment.get('extendData', 'null')
                                if comment_extend != 'null':
                                    comment_reply_num = comment_extend.get('replyNum', 0)

                                comment_reply_list = []
                                if comment_reply_num > 0:
                                    for comment_reply in comment['replies']:
                                        comment_reply_content = comment_reply['content']
                                        # 去掉 @{uin:117557,nick:16,who:1,auto:1} 这种文字
                                        comment_reply_content = \
                                            re.subn(re.compile('\@\{.*?\}'), '', comment_reply_content)[
                                                0].strip()
                                        comment_reply_name = comment_reply['poster']['name']
                                        comment_reply_time = comment_reply['postTime']
                                        comment_reply_list.append(dict(comment_reply_content=comment_reply_content,
                                                                       comment_reply_name=comment_reply_name,
                                                                       comment_reply_time=comment_reply_time))

                            cmt_total_num += comment_reply_num
                            cmt_list.append(
                                dict(comment_content=comment_content, comment_name=comment_name,
                                     comment_time=comment_time,
                                     comment_reply_num=comment_reply_num, comment_reply_list=comment_reply_list))
                        except BaseException as e:
                            self.format_error(e, comment)

                self.mood_data.append(dict(tid=tid, content=content, time=time, time_stamp=time_stamp, pic_num=pic_num,
                                           cmt_num=cmt_num,
                                           cmt_total_num=cmt_total_num,
                                           cmt_list=cmt_list))
        except BaseException as e:
            self.format_error(e, "Error in parse mood:" + str(msglist))
            self.mood_data.append(dict(tid=tid, content=msglist['message'], time="", time_stamp="", pic_num=0,
                                       cmt_num=0,
                                       cmt_total_num=0,
                                       cmt_list=[], friend_num=-1))

    def parse_like_and_prd(self, like):
        try:
            data = like['data'][0]
            current = data['current']
            key = current['key'].split('/')[-1]
            newdata = current['newdata']
            # 点赞数
            if 'LIKE' in newdata:
                like_num = newdata['LIKE']
                # 浏览数
                prd_num = newdata['PRD']

                if self.debug:
                    if key == like['tid']:
                        print("correct like tid")
                    else:
                        print("wrong like tid")
                self.like_detail_df.append(dict(tid=like['tid'], like_num=like_num, prd_num=prd_num))
            else:
                self.like_detail_df.append(dict(tid=like['tid'], like_num=0, prd_num=0))
        except BaseException as e:
            print(like)
            self.format_error(e, 'Error in like, return 0')
            self.like_detail_df.append(dict(tid=like['tid'], like_num=0, prd_num=0))

    def parse_like_names(self, like):
        try:
            data = like['data']
            total_num = data['total_number']
            like_uin_info = data['like_uin_info']
            uin_list = []

            for uin in like_uin_info:
                nick = uin['nick']
                gender = uin['gender']
                uin_list.append(dict(nick=nick, gender=gender))
            self.like_list_names_df.append(dict(total_num=total_num, uin_list=uin_list, tid=like['tid']))
        except BaseException as e:
            self.format_error(e, "Error in parse like names")
            self.like_list_names_df.append(dict(total_num=0, uin_list=[], tid=like['tid']))

    def export_mood_df(self, export_csv=True, export_excel=True, export_mongodb=True):
        """
        导出csv和excel表或到mongodb中
        :param export_csv:
        :param export_excel:
        :param export_mongodb:
        :return:
        """
        if self.has_clean_data:
            if export_csv:
                self.save_data_to_csv()
            if export_excel:
                self.save_data_to_excel()
            if export_mongodb:
                self.save_data_to_mongodb()
            print("导出数据成功", self.username)
        else:
            print('数据未清洗，无法导出')

    def calculate_history_like_agree(self):
        """
        计算历史上每条说说的内容、点赞量和评论量
        :return:
        """
        if not self.has_clean_data:
            self.get_useful_info_from_json()
        history_df = self.mood_data_df.loc[:, ['cmt_total_num', 'like_num', 'content', 'time']]
        history_json = history_df.to_json(orient='records', force_ascii=False)
        if self.use_redis:
            self.re.set(self.history_like_agree_file_name, json.dumps(history_json, ensure_ascii=False))
            if not self.no_delete:
                self.re.expire(self.history_like_agree_file_name, EXPIRE_TIME_IN_SECONDS)
        else:
            self.save_data_to_json(history_json, self.history_like_agree_file_name)

