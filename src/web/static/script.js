let history_dom = echarts.init(document.getElementById(HISTORY_DOM));

avalon.config({
    interpolate: ['{$', '$}']
});
let vm = avalon.define({
    $id: 'qqzone',
    nick_name: '',
    // 请修改为对应的前端地址
    QQ_LOCATION: QQ_FRONT_LOCATION + '/data/userinfo/',
    qq_id: '',
    stop_num: -1,
    stop_date: '-1',
    no_delete: false,
    agree: false,
    disable: 'false',
    qq_cookie: '',
    begin_spider: 0,
    spider_info: [],
    true_mood_num: -1,
    spider_num: 0,
    show_process: 0,
    process_width: 0,
    spider_text: SPIDER_TEXT.DOING,
    spider_state: SPIDER_STATE.CONFIG,
    clean_data_text: CLEAN_DATA_TEXT.DOING,
    data_state: CLEAN_DATA_STATE.DOING,
    visual_data_url: '',
    password: '',
    view_data_state: VIEW_DATA_STATE.config,
    image_path: 'static/image/',
    friend_info_spider_text: SPIDER_FRIEND_TEXT.DOING,
    spider_friend_num: 0,
    friend_process_width: 0,
    all_friend_num: 0,
    friend_info_spider_state: SPIDER_FRIEND_STATE.DOING,
    user: {},
    qr_code_path: '',
    login_state: LOGIN_STATE.UNLOGIN,
    force_stop: false,
    waiting_user_num: 0,
    finish_user_num: 0,
    warm_tip: WARM_TIP[0],
    warm_tip_index: 0,
    init_parameter: function () {
        vm.spider_text = SPIDER_TEXT.DOING;
        vm.spider_state = SPIDER_STATE.CONFIG;
        vm.clean_data_text = CLEAN_DATA_TEXT.DOING;
        vm.data_state = CLEAN_DATA_STATE.DOING;
        vm.view_data_state = VIEW_DATA_STATE.config;
        vm.friend_info_spider_text = SPIDER_FRIEND_TEXT.DOING;
        vm.spider_friend_num = 0;
        vm.friend_process_width = 0;
        vm.all_friend_num = 0;
        vm.spider_num = 0;
        vm.process_width = 0;
        vm.true_mood_num = -1;
        vm.friend_info_spider_state = SPIDER_FRIEND_STATE.DOING;
        vm.user = {};
        vm.spider_info = [];
        vm.login_state = LOGIN_STATE.UNLOGIN;
        vm.agree = false;
        vm.qr_code_path = '';
        vm.query_finish_num();

    },

    query_finish_num: function () {
        $.ajax({
            url: 'spider/query_finish_user_num',
            success: function (res) {
                data = JSON.parse(res);
                vm.finish_user_num = data.finish_user_num;
                vm.waiting_user_num = data.waiting_user_num;
            }
        });
    },

    change_tip: function () {
        var tip_len = WARM_TIP.length;
        if (vm.warm_tip_index < tip_len - 1) {
            vm.warm_tip_index += 1;
        } else {
            vm.warm_tip_index = 0;
        }
        vm.warm_tip = WARM_TIP[vm.warm_tip_index];
    },
    force_stop_spider: function () {
        var message = confirm("该操作会强制停止爬虫，导致数据丢失(之后您可以重新启动爬虫)，您确认要进行该操作吗？");
        if (message) {
            $.ajax({
                url: "spider/stop_spider_force/" + vm.qq_id + '/' + sha1(vm.password),
                type: 'GET',
                success: function (res) {
                    clearInterval(vm.query_num);
                    clearInterval(vm.query_friend_info);
                    vm.spider_text = SPIDER_TEXT.STOP;
                    vm.init_parameter();
                    vm.force_stop = true;
                    alert("停止爬虫成功");
                }
            })
        }
    },
    view_data: function () {
        if (vm.qq_id.length === 0 && vm.password.length === 0 && vm.nick_name.length === 0) {
            alert("QQ号、用户名和校验码不能为空");
        } else {
            $.ajax({
                url: '/data/userinfo/' + vm.qq_id + '/' + vm.nick_name + '/' + sha1(vm.password),
                type: 'GET',

                success: function (res) {
                    res = JSON.parse(res);
                    if (res.finish) {
                        vm.view_data_state = VIEW_DATA_STATE.data;
                        vm.user = res.user;
                        vm.fetch_history_data();
                        vm.show_cloud_image();
                    } else {
                        alert("暂无该用户数据, 请先运行爬虫");
                    }
                    clearInterval(vm.query_num);
                }
            })
        }
    },

    view_new_data: function() {
        if (vm.qq_id.length === 0 && vm.password.length === 0 && vm.nick_name.length === 0) {
            alert("QQ号、用户名和校验码不能为空");
        } else {
            $.ajax({
                url: '/data/userinfo/' + vm.qq_id + '/' + vm.nick_name + '/' + sha1(vm.password),
                type: 'GET',

                success: function (res) {
                    res = JSON.parse(res);
                    if (res.finish) {
                        $(window).attr('location', vm.QQ_LOCATION +vm.qq_id +"/" +vm.nick_name + "/" + vm.encrypt())
                    } else {
                        alert("暂无该用户数据, 请先运行爬虫");
                    }
                    clearInterval(vm.query_num);
                }
            });

        }
    },

    submit_data: function () {
        if (vm.qq_id !== '' && vm.nick_name !== '') {
            if (agree) {
                $.ajax({
                    url: "/spider/start_spider",
                    type: 'post',
                    data: {
                        nick_name: vm.nick_name,
                        qq: vm.qq_id,
                        stop_time: vm.stop_date,
                        mood_num: vm.stop_num,
                        no_delete: vm.no_delete,
                        password: sha1(vm.password)
                    },
                    success: function (data) {
                        data = JSON.parse(data);
                        vm.force_stop = false;
                        if (data.result === SUCCESS_STATE) {
                            //alert("success");
                            vm.begin_spider = 1;
                            vm.spider_state = SPIDER_STATE.SPIDER;
                            vm.friend_info_spider_state = SPIDER_FRIEND_STATE.DOING;
                            vm.query_interval = setInterval(function () {
                                vm.query_spider_info(vm.qq_id);
                            }, 1000);
                        } else if (data.result === CHECK_COOKIE) {
                            alert("请输入有效cookie");
                        } else if (data.result === WAITING_USER_STATE) {
                            alert("当前有" + data.waiting_num + "位用户正在使用爬虫，请大约" + 5 * data.waiting_num + "分钟后再尝试");
                        } else if (data.result === ALREADY_IN) {
                            alert("您的账号已经在后台爬取");
                            vm.begin_spider = 1;
                            vm.spider_state = SPIDER_STATE.SPIDER;
                            vm.login_state = LOGIN_STATE.LOGIN_SUCCESS;
                            vm.friend_info_spider_state = SPIDER_FRIEND_STATE.DOING;
                            if (data.friend_num){
                                vm.all_friend_num = data.friend_num;
                            }
                            if (data.mood_num){
                                vm.true_mood_num = data.mood_num;
                            }
                            // 开始轮询好友数量
                            vm.query_friend_info = setInterval(function () {
                                vm.query_friend_info_num(vm.qq_id);
                            }, 500);
                            // 开始轮询说说数量
                            vm.query_num = setInterval(function () {
                                vm.query_spider_num(vm.qq_id);
                            }, 1000);
                        } else {
                            alert("未知错误:" + data.result)
                        }
                    }
                })
            }
        } else {
            alert("昵称、QQ号、Cookie不能为空");
        }

    },

    // 查询爬虫从登陆到获取主页信息的状态
    // 在获取到好友数量后停止轮询
    query_spider_info: function (QQ) {
        $.ajax({
            url: '/spider/query_spider_info/' + QQ + '/' + sha1(vm.password),
            type: 'GET',
            success: function (data) {
                data = JSON.parse(data);
                if (data.info.length > 2 && data.finish !== LOGGING_STATE) {
                    vm.login_state = LOGIN_STATE.LOGIN_SUCCESS;
                    vm.spider_info.push(data.info);
                }
                if (data.finish === SUCCESS_STATE) {

                    vm.show_process = 1;
                    if (data.mood_num) {
                        vm.true_mood_num = data.mood_num;
                    }
                    // 不知是什么原因，在docker中，会出现finish =1，但是mood_num == -1的失败情况
                    // 出现这种情况后就强制停止爬虫
                    if (data.mood_num === -1) {
                        clearInterval(vm.query_interval);
                        $.ajax({
                            url: '/spider/stop_spider_force/' + vm.qq_id + '/' + sha1(vm.password),
                            type: 'get',
                            success: function (data) {
                                console.log(data);
                            }
                        });
                        vm.init_parameter();
                        alert("由于网络等原因，获取数据失败，请稍后再尝试");
                    }
                } else if (data.finish === FINISH_FRIEND) {

                    if (data.friend_num) {
                        vm.all_friend_num = data.friend_num;
                    }

                    // 停止spider_info的轮询
                    clearInterval(vm.query_interval);
                    // 开始轮询好友数量
                    vm.query_friend_info = setInterval(function () {
                        vm.query_friend_info_num(vm.qq_id);
                    }, 500);
                    // 开始轮询说说数量
                    vm.query_num = setInterval(function () {
                        vm.query_spider_num(vm.qq_id);
                    }, 1000);
                } else if (data.finish === FAILED_STATE) {
                    clearInterval(vm.query_interval);
                    vm.show_process = 0;
                    vm.spider_state = SPIDER_STATE.CONFIG;
                    vm.init_parameter();
                    vm.login_state = LOGIN_STATE.LOGIN_FAIELD;

                    alert(data.info);
                } else if (data.finish === INVALID_LOGIN) {
                    clearInterval(vm.query_interval);
                    vm.init_parameter();
                    alert(WRONG_PASSWORD_QQ);
                } else if (data.finish === LOGGING_STATE) {
                    vm.login_state = LOGIN_STATE.LOGINING;
                    vm.qr_code_path = 'static/image/qr' + data.info;
                    var image = new Image;
                    $('#qr_container').empty();
                    $('#qr_container').append(image);
                    image.src = vm.qr_code_path;
                } else if (data.finish === NOT_MATCH_STATE) {
                    clearInterval(vm.query_interval);
                    vm.init_parameter();
                    alert("输入的QQ号与扫码登陆的不一致 \n为保证用户的信息安全，我们只提供用户爬取自己的空间数据的功能！");
                }
            }
        })
    },
    stop_spider: function () {
        //避免用户多次点击停止爬虫导致保存数据出错
        if (vm.spider_text !== SPIDER_TEXT.STOP) {
            clearInterval(vm.query_num);
            clearInterval(vm.query_friend_info);
            vm.spider_text = SPIDER_TEXT.STOP;
            $.ajax({
                url: '/spider/stop_spider/' + vm.qq_id + '/' + sha1(vm.password),
                type: 'get',
                success: function (data) {
                    data = JSON.parse(data);
                    if (data.finish !== -2) {
                        vm.spider_state = SPIDER_STATE.FINISH;
                        vm.spider_num = parseInt(data.num);
                        vm.process_width = Math.ceil(parseInt(vm.spider_num) / parseInt(vm.true_mood_num) * 100) + "%";
                        vm.spider_friend_num = parseInt(data.friend_num);
                        vm.friend_process_width = Math.ceil(parseInt(vm.spider_friend_num) / parseInt(vm.all_friend_num) * 100) + "%";
                        vm.query_clean_state();
                    } else {
                        alert(WRONG_PASSWORD_QQ);
                    }
                }
            });
        }
    },
    query_spider_num: function (QQ) {
        $.ajax({
            url: '/spider/query_spider_num/' + QQ + '/' + vm.true_mood_num + '/' + sha1(vm.password),
            type: 'GET',
            success: function (data) {
                data = JSON.parse(data);
                vm.spider_num = data.num;
                vm.process_width = Math.ceil(parseInt(vm.spider_num) / parseInt(vm.true_mood_num) * 100) + "%";
                if (data.finish === 1) {
                    clearInterval(vm.query_num);
                    vm.spider_state = SPIDER_STATE.FINISH;
                    vm.data_state = CLEAN_DATA_STATE.DOING;
                    vm.query_clean_state();
                } else if (data.finish === -2) {
                    if (!vm.force_stop) {
                        alert(WRONG_PASSWORD_QQ);
                        clearInterval(vm.query_num);
                    }

                }

            }
        })
    },

    query_friend_info_num: function (QQ) {
        $.ajax({
            url: '/spider/query_friend_info_num/' + QQ + '/' + vm.all_friend_num + '/' + sha1(vm.password),
            type: 'GET',
            success: function (data) {
                data = JSON.parse(data);
                vm.spider_friend_num = data.num;
                vm.friend_process_width = Math.ceil(parseInt(vm.spider_friend_num) / parseInt(vm.all_friend_num) * 100) + "%";
                if (data.finish === 1) {
                    clearInterval(vm.query_friend_info);
                    vm.friend_info_spider_state = SPIDER_FRIEND_STATE.FINISH;
                } else if (data.finish === -2) {
                    if (!vm.force_stop) {
                        alert(WRONG_PASSWORD_QQ);
                        clearInterval(vm.query_friend_info);
                    }

                }
            }
        })
    },
    query_clean_state: function () {
        $.ajax({
            url: '/spider/query_clean_data/' + vm.qq_id + '/' + sha1(vm.password),
            type: 'GET',
            success: function (data) {
                data = JSON.parse(data);
                if (data.finish === '1') {
                    vm.data_state = CLEAN_DATA_STATE.FINISH;

                } else if (data.finish === -2) {
                    alert(WRONG_PASSWORD_QQ);
                }
            }
        })
    },

    show_cloud_image: function () {
        var image1 = new Image;
        var cloud_img1 = $('#like_cloud_img');
        cloud_img1.empty();
        cloud_img1.append(image1);
        image1.src = vm.image_path + vm.qq_id + "/" + vm.qq_id + "_cmt.jpg";
        image1.alt = "经常给您评论的好友们";
        var cloud_img2 = $('#cmt_cloud_img');
        var image2 = new Image;
        cloud_img2.empty();
        cloud_img2.append(image2);
        image2.src = vm.image_path + vm.qq_id + "/" + vm.qq_id + "_like.jpg";
        image2.alt = "经常给您点赞的好友们";
        // var cloud_img3 = $('#content_cloud_img');
        // var image3 = new Image;
        // cloud_img3.append(image3);
        // image3.src = vm.image_path + vm.qq_id + "/" + vm.qq_id+ "_content.jpg";
        // image2.alt="您QQ空间说说的关键字";
    },
    clear_cache: function () {
        clearInterval(vm.query_num);
        $.ajax({
            url: '/data/clear_cache/' + vm.qq_id + '/' + sha1(vm.password),
            type: 'GET',
            success: function (data) {
                data = JSON.parse(data);
                if (data.finish) {
                    vm.init_parameter();
                    alert("清除缓存成功");
                } else {
                    alert(data.info);
                }
            }
        })
    },
    fetch_history_data: function () {
        $.ajax({
            url: '/data/get_history/' + vm.qq_id + '/' + vm.nick_name + '/' + sha1(vm.password),
            success: function (result) {
                result = JSON.parse(result);
                if (result.finish) {
                    data = result.data;
                    draw_history_line(history_dom, data, "QQ空间说说历史曲线图");
                }

            }
        })
    },
    encrypt: function () {
        return sha1(vm.password);
    },

    return_config: function () {
        vm.view_data_state = VIEW_DATA_STATE.config;
    },
    judge_stop_friend_processbar: function () {
        return vm.spider_state === SPIDER_STATE.SPIDER && vm.spider_friend_num < vm.all_friend_num;
    }
});

vm.$watch("agree", function (new_v, old_v) {
    switch (new_v) {
        case true:
            $('#start_get').removeAttr("disabled");
            break;
        case false:
            $('#start_get').attr("disabled", "true");
            break;
    }
});

vm.$watch("spider_state", function (new_v, old_v) {
    switch (new_v) {
        case SPIDER_STATE.FINISH:
            vm.spider_text = SPIDER_TEXT.FINISH;
            $('#start_get').removeAttr("disabled");
            $('#delete_cache').removeAttr("disabled");
            $('#qq_id').removeAttr("disabled");
            $('#view_data').removeAttr("disabled");
            vm.agree = false;
            break;
        case SPIDER_STATE.SPIDER:
            vm.spider_text = SPIDER_TEXT.DOING;
            $('#start_get').attr("disabled", "true");
            $('#delete_cache').attr("disabled", "true");
            $('#password').attr("disabled", "true");
            $('#qq_id').attr("disabled", "true");
            $('#view_data').attr("disabled", "true");
            break;
        case SPIDER_STATE.CONFIG:
            $('#password').removeAttr("disabled");
            $('#qq_id').removeAttr("disabled");
            $('#view_data').removeAttr("disabled");
    }
});

vm.$watch("data_state", function (new_v, old_v) {
    switch (new_v) {
        case CLEAN_DATA_STATE.DOING:
            vm.clean_data_text = CLEAN_DATA_TEXT.DOING;
            break;
        case CLEAN_DATA_STATE.FINISH:
            vm.clean_data_text = CLEAN_DATA_TEXT.FINISH;
            break;
    }
});

vm.$watch("friend_info_spider_state", function (new_v, old_v) {
    switch (new_v) {
        case SPIDER_FRIEND_STATE.DOING:
            vm.friend_info_spider_text = SPIDER_FRIEND_TEXT.DOING;
            break;
        case SPIDER_FRIEND_STATE.FINISH:
            vm.friend_info_spider_text = SPIDER_FRIEND_TEXT.FINISH;
    }
});

$(document).ready(function () {
    vm.query_finish_num();
    setInterval(function () {
        vm.query_finish_num();
    }, 10000);

    setInterval(function () {
        vm.change_tip();
    }, 6000);
});
