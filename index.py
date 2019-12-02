import pandas as pd
import numpy as np
import logging
import json
import tablestore

# define ots instance
endpoint = 'https://instance.cn-hangzhou.ots.aliyuncs.com'
access_key_id = 'LTAI4FuP4exR6UYDZ3M8nGMb'
access_key_secret = 'cNurjm500jSSG1AO4bsPBiOOovKiPu'
instance_name = 'env-device'
ots_client = tablestore.OTSClient(
    endpoint, access_key_id, access_key_secret,
    instance_name, logger_name='table_store.log',
    retry_policy=tablestore.WriteRetryPolicy()
)


# 处理文本匹配的查询缺失值情况
def compose_query_re(inp):
    if len(inp) == 1:  # case no require is make
        return '?*'
    else:
        return inp


# 将table store返回数据格式标准化为dataframe
def tb2df(rows):
    result = []
    for row in rows:
        temp = {}
        # first handle key
        for k in row[0]:
            temp[k[0]] = k[1]
        # now handle columns
        for c in row[1]:
            temp[c[0]] = c[1]
        result.append(temp.copy())
    return pd.DataFrame(result)


# 将df对象处理为iot studio中可识别的json格式
def df2iot(df):
    # first calculate total
    total = df.shape[0]
    result = {"total": total, "list":[]}
    # now load list info
    for i in df.index:
        result['list'].append(list(df.iloc[i,:].values))
    return result


def device_search_handler(environ, start_response):
    context = environ['fc.context']
    request_uri = environ['fc.request_uri']
    logger = logging.getLogger()
    for k, v in environ.items():
        if k.startswith('QUERY_'):
            content = v.split('&')
            deviceType = content[0].split('=')[1]
            village = content[1].split('=')[1]
            # logger.info('key is %s, values is %s' % (deviceType, village))
    # try:
    #     request_body_size = int(environ.get('CONTENT_LENGTH', 0))
    # except (ValueError):
    #     request_body_size = 0
    # request_body = environ['wsgi.input'].read(request_body_size)
    # logger.info('a%s' % request_body)
    # compose query
    query = tablestore.BoolQuery(
        must_queries=[
            tablestore.WildcardQuery('DeviceType', compose_query_re(deviceType)),
            tablestore.WildcardQuery('Village', compose_query_re(village))
        ]
    )
    rows, next_token, total_count, is_all_succeed = ots_client.search(
        'device', 'device_search',
        tablestore.SearchQuery(query, limit=100, get_total_count=True),
        tablestore.ColumnsToGet(return_type=tablestore.ColumnReturnType.ALL)
    )
    # compose response
    df = tb2df(rows)
    status = '200 OK'
    response_headers = [('Content-type', 'text/plain')]
    start_response(status, response_headers)
    result = df2iot(df)
    return [str(result).replace("'",'"').encode(encoding='utf-8')]


def device_list_handler(environ, start_response):
    context = environ['fc.context']
    request_uri = environ['fc.request_uri']
    logger = logging.getLogger()
    for k, v in environ.items():
        if k.startswith('QUERY_'):
            content = v.split('&')
            col = content[0].split('=')[1]
            # logger.info('key is %s, values is %s' % (deviceType, village))

    # compose query
    query = tablestore.WildcardQuery('DeviceType', '?*')
    rows, next_token, total_count, is_all_succeed = ots_client.search(
        'device', 'device_search',
        tablestore.SearchQuery(query, limit=100, get_total_count=True),
        tablestore.ColumnsToGet(return_type=tablestore.ColumnReturnType.ALL)
    )
    # compose response
    df = tb2df(rows)
    # now retrieve only device name
    data = pd.unique(df[col])
    # now we format data into needed form
    result = []
    for d in data:
        temp = {
            "label": d,
            "value": d
        }
        result.append(temp.copy())
    status = '200 OK'
    response_headers = [('Content-type', 'text/plain')]
    start_response(status, response_headers)
    return [str(result).replace("'", '"').encode(encoding='utf-8')]


def debug_handler(environ, start_response):
    context = environ['fc.context']
    request_uri = environ['fc.request_uri']
    logger = logging.getLogger()
    for k, v in environ.items():
        if k.startswith('QUERY_'):
            content = v.split('&')
            info = content[0].split('=')[1]
            # logger.info('key is %s, values is %s' % (deviceType, village))

    # compose query
    primary_key = [('id', 1)]
    a_col = [('value', info)]
    row = tablestore.Row(primary_key, a_col)
    condition = tablestore.Condition(tablestore.RowExistenceExpectation.EXPECT_NOT_EXIST)
    # compose response
    try:
        # 调用put_row方法，如果没有指定ReturnType，则return_row为None。
        consumed, return_row = ots_client.put_row('debug', row, condition)
        # 打印出此次请求消耗的写CU。
        logger.info('put row succeed, consume %s write cu.' % consumed.write)
        # 客户端异常，一般为参数错误或者网络异常。
    except tablestore.OTSClientError as e:
        logger.info("put row failed, http_status:%d, error_message:%s" % (e.get_http_status(), e.get_error_message()))
    # 服务端异常，一般为参数错误或者流控错误。
    except tablestore.OTSServiceError as e:
        logger.info("put row failed, http_status:%d, error_code:%s, error_message:%s, request_id:%s" % (
        e.get_http_status(), e.get_error_code(), e.get_error_message(), e.get_request_id()))
    status = '200 OK'
    response_headers = [('Content-type', 'text/plain')]
    start_response(status, response_headers)
    return ['insert finished'.encode(encoding='utf-8')]
