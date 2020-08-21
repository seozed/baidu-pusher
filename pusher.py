from multiprocessing.dummy import Pool as ThreadPool
import requests
import logging
from more_itertools import chunked
from functools import partial
import click

logname = 'run.log'
logging.basicConfig(filename=logname,
                    filemode='a',
                    format='%(asctime)s Method %(funcName)s %(levelname)s: %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)
logging.info("Running Urban Planning")
logger = logging.getLogger('the loger name')

session = requests.Session()


def post(url, data, headers=None):
    _headers = {'Content-Type': 'text/plain'}
    if headers:
        _headers.update(headers)
    response = session.post(url, data=data, headers=_headers)
    logger.info(f'url:{url.strip()} message:{response.text}')
    return response


def get(url, headers=None):
    _headers = {'Content-Type': 'text/plain'}
    if headers:
        _headers.update(headers)
    response = session.get(url, headers=_headers)
    logger.info(f'url:{url.strip()} message:{response.text if response.text else response.status_code}')
    return response


def valid_amount(links, amount=2000):
    if isinstance(links, list) and len(links) > amount:
        raise ValueError('数值超出上限，单次提交URL上限为%s条' % amount)


def push_of_javascript(url, **kwargs):
    """
    javascript推送, HTTPS版
    :param url:
    :return:
    """
    if isinstance(url, list):
        url = url.pop()

    if isinstance(url, bytes):
        url = url.decode()
    url = url.strip()
    url = f"https://sp0.baidu.com/9_Q4simg2RQJ8t7jm9iCKT-xh_/s.gif?l={url}"
    return get(url)


def push_realtime_for_pc(links, site, token, action='urls', **kwargs):
    """
    PC端实时推送
    :param links: 链接，单次最多2000条
    :param action: urls|del|update
    :param site: full domain
    :param token: token
    :return: HTTP Response
    """
    valid_amount(links)

    assert all([site, token])

    if not site.startswith('https://'):
        site = 'https://' + site

    payload = ''.join(links)
    headers = {'Content-Type': 'text/plain'}

    url = f"http://data.zz.baidu.com/{action}?site={site}&token={token}"
    logger.info('push %s urls' % len(links))
    return post(url, data=payload, headers=headers)


def batch_of_mobile(links: list, appid, token, **kwargs):
    """
    百度移动专区周级收录推送接口
    :param links: 链接列表，单次最多2000条
    :return: HTTP Response
    """
    valid_amount(links)
    assert all([appid, token])
    url = f'http://data.zz.baidu.com/urls?appid={appid}&token={token}&type=batch'
    headers = {
        "Content-Type": 'text/plain'
    }
    payload = ''.join(links)
    logger.info('push %s urls' % len(links))
    return post(url, headers=headers, data=payload)


def factory(func, url, **kwargs):
    return func(url, **kwargs)


def pop_url(chunked_list):
    for l in chunked_list:
        yield l.pop()


# =======================================================================================
# Command line config
# =======================================================================================
@click.group()
def cli():
    pass


@click.command()
@click.argument('urls', type=click.File('rb'))
def js(urls):
    """
    自动推送，使用javascript接口推送到百度
    :param urls:
    :return:
    """
    per_quantity = 1
    chunked_list = pop_url(chunked(urls, per_quantity))
    pool = ThreadPool(10)
    results = pool.map(push_of_javascript, chunked_list)
    pool.close()
    pool.join()


@click.command()
@click.argument('urls', type=click.File('r'))
@click.argument('site')
@click.argument('token')
@click.option('--action', type=click.Choice(['urls', 'update', 'del']), default='urls', show_default=True, help='操作类型')
def realtime(urls: open, action, site, token):
    """
    主动推送（实时）

    :param urls: URL文件，一行一个

    :param site: 在搜索资源平台验证的站点，比如https://www.17liuxue.com

    :param token: 在搜索资源平台申请的推送用的准入密钥
    """
    per_quantity = 2000
    chunked_list = chunked(urls, per_quantity)
    func = partial(factory, push_realtime_for_pc, action=action, site=site, token=token)

    pool = ThreadPool(10)
    results = pool.map(func, chunked_list)
    pool.close()
    pool.join()


@click.command()
@click.argument('urls', type=click.File('r'))
@click.argument('appid')
@click.argument('token')
@click.option('--action', default='urls', show_default=True)
@click.option('--type', default='batch', show_default=True, help="对提交内容的数据类型说明，周级收录参数：batch")
def week_mobile(urls: open, appid, token, action, type):
    """
    移动平台周级推送接口

    :param urls: URL文件， 一行一个

    :param appid: 您的唯一识别ID

    :param token: 在搜索资源平台申请的推送用的准入密钥

    """
    per_quantity = 2000
    url_list = chunked(urls, per_quantity)
    func = partial(factory, batch_of_mobile, action=action, appid=appid, token=token)

    pool = ThreadPool(10)
    results = pool.map(func, url_list)
    pool.close()
    pool.join()


cli.add_command(js)
cli.add_command(realtime)
cli.add_command(week_mobile)

if __name__ == '__main__':
    cli()
