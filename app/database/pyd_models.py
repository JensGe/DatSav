from typing import List
from app.common import enum
from app.common import common_values as c

from pydantic import BaseModel, HttpUrl
from uuid import UUID
from datetime import datetime


class BasisModel(BaseModel):
    class Config:
        orm_mode = True


# Frontier
class FrontierRequest(BasisModel):
    crawler_uuid: UUID
    amount: int = c.frontier_amount
    length: int = c.frontier_length
    short_term_mode: enum.STF = enum.STF.random
    long_term_mode: enum.LTF = enum.LTF.random


class Url(BasisModel):
    url: HttpUrl
    fqdn: str

    url_last_visited: datetime = None
    url_blacklisted: bool = None
    url_bot_excluded: bool = None


class UrlFrontier(BasisModel):
    fqdn: str
    tld: enum.TLD = None

    fqdn_last_ipv4: str = None
    fqdn_last_ipv6: str = None

    fqdn_pagerank: float = None
    fqdn_crawl_delay: int = None
    fqdn_url_count: int = None

    url_list: List[Url] = []


class URLReference(BasisModel):
    url_out: str
    url_in: str
    date: datetime


class FrontierResponse(BasisModel):
    uuid: str
    response_url: HttpUrl = None
    latest_return: datetime = None
    url_frontiers_count: int = c.url_frontier_count
    urls_count: int = c.urls_count
    url_frontiers: List[UrlFrontier] = []


class SubmitFrontier(BasisModel):
    uuid: str
    url_frontiers_count: int = c.url_frontier_count
    urls_count: int = c.urls_count
    url_frontiers: List[UrlFrontier] = []