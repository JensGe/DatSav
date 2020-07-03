from typing import List
from app.common import enum
from app.common import common_values as c

from pydantic import BaseModel, HttpUrl, EmailStr
from uuid import UUID
from datetime import datetime


class BasisModel(BaseModel):
    class Config:
        orm_mode = True


# Fetcher
class Fetcher(BasisModel):
    uuid: UUID
    fetcher_hashes: List[int] = []

    contact: EmailStr
    name: str

    reg_date: datetime
    location: str = None
    tld_preference: str = None


class CreateFetcher(BasisModel):
    contact: EmailStr
    name: str
    location: str = None
    tld_preference: str = None


class UpdateFetcher(BasisModel):
    uuid: UUID
    contact: EmailStr = None
    name: str = None
    location: str = None
    tld_preference: str = None


class DeleteFetcher(BasisModel):
    uuid: UUID


# Frontier
class FrontierRequest(BasisModel):
    fetcher_uuid: UUID

    amount: int = c.frontier_amount
    length: int = c.frontier_length

    short_term_prio_mode: enum.SHORTPRIO = enum.SHORTPRIO.random
    long_term_prio_mode: enum.LONGPRIO = enum.LONGPRIO.random
    long_term_part_mode: enum.LONGPART = enum.LONGPART.none


class Url(BasisModel):
    url: HttpUrl
    fqdn: str

    url_pagerank: float = None
    url_discovery_date: datetime = None
    url_last_visited: datetime = None
    url_blacklisted: bool = None
    url_bot_excluded: bool = None


class Frontier(BasisModel):
    fqdn: str
    tld: str = None

    fqdn_hash: int = None
    fqdn_hash_fetcher_index: int = None

    fqdn_last_ipv4: str = None
    fqdn_last_ipv6: str = None

    fqdn_avg_pagerank: float = None
    fqdn_crawl_delay: int = None
    fqdn_url_count: int = None

    url_list: List[Url] = []


class URLReference(BasisModel):
    url_out: str
    url_in: str
    date: datetime


class FrontierResponse(BasisModel):
    uuid: str

    short_term_prio_mode: enum.SHORTPRIO = None
    long_term_prio_mode: enum.LONGPRIO = None
    long_term_part_mode: enum.LONGPART = None

    response_url: HttpUrl = None
    latest_return: datetime = None

    url_frontiers_count: int = c.url_frontier_count
    urls_count: int = c.urls_count

    url_frontiers: List[Frontier] = []


class SubmitFrontier(BasisModel):
    uuid: str
    fqdn_count: int
    fqdns: List[Frontier]
    url_count: int
    urls: List[Url] = []


# Developer Tools
class GenerateRequest(BasisModel):
    fetcher_amount: int = c.fetcher
    fqdn_amount: int = c.fqdn_amount
    min_url_amount: int = c.min_url
    max_url_amount: int = c.max_url

    visited_ratio: float = c.visited_ratio
    connection_amount: int = c.connections
    fixed_crawl_delay: int = None


class StatsResponse(BasisModel):
    fetcher_amount: int
    frontier_amount: int

    url_amount: int
    url_ref_amount: int
    reserved_fqdn_amount: int

    avg_freshness: str
    visited_ratio: float
    fqdn_hash_range: float


class DeleteDatabase(BasisModel):
    delete_url_refs: bool = False
    delete_fetcher_hashes: bool = False
    delete_fetchers: bool = False
    delete_urls: bool = False
    delete_fqdns: bool = False
    delete_reserved_fqdns: bool = False


class GetRandomUrls(BasisModel):
    amount: int = 1
    fqdn: str = None


class RandomUrls(BasisModel):
    url_list: List[Url] = []


class FetcherSettings(BasisModel):
    logging_mode: int = 20
    crawling_speed_factor: float = 10
    default_crawl_delay: int = 5
    parallel_process: int = 12
    parallel_fetcher: int = 1

    iterations: int = 1
    fqdn_amount: int = 10
    url_amount: int = 0

    short_term_prio_mode: enum.SHORTPRIO = enum.SHORTPRIO.random
    long_term_prio_mode: enum.LONGPRIO = enum.LONGPRIO.random
    long_term_part_mode: enum.LONGPART = enum.LONGPART.none

    min_links_per_page: int = 1
    max_links_per_page: int = 1
    lpp_distribution_type: enum.PAGELINKDISTR = enum.PAGELINKDISTR.discrete

    internal_vs_external_threshold: float = 1.0
    new_vs_existing_threshold: float = 1.0


class SimulatedParsedList(BasisModel):
    uuid: str
    fqdn_count: int
    fqdns: List[Frontier]
    url_count: int
    urls: List[Url]