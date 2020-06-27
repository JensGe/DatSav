from app.database import pyd_models, db_models

from sqlalchemy.orm import Session
from typing import List


def fqdn_exists(db: Session, fqdn):
    if (
        db.query(db_models.Frontier)
        .filter(db_models.Frontier.fqdn == fqdn)
        .count()
        > 0
    ):
        return True
    return False


def url_exists(db: Session, url):
    if (
        db.query(db_models.Url).filter(db_models.Url.url == url).count()
        > 0
    ):
        return True
    return False


def get_tld(fqdn):
    if fqdn.split(".")[-2] == "co":
        tld = ".".join(fqdn.split(".")[-2:])
    else:
        tld = fqdn.split(".")[-1]
    return tld


def create_fqdn_lists(db: Session, fqdns: List[pyd_models.Frontier]):
    fqdn_insert_list = []
    fqdn_update_list = []

    fetcher_amount = db.query(db_models.Fetcher).count()


    for fqdn in fqdns:
        if not fqdn_exists(db, fqdn.fqdn) and fqdn.fqdn not in [
            url.fqdn for url in fqdn_insert_list
        ]:
            fetcher_idx = hash(fqdn.fqdn) % fetcher_amount if fetcher_amount != 0 else None
            fqdn_insert_list.append(
                db_models.Frontier(
                    fqdn=fqdn.fqdn,
                    fetcher_idx=fetcher_idx,
                    tld=fqdn.tld,
                    fqdn_last_ipv4=fqdn.fqdn_last_ipv4,
                    fqdn_last_ipv6=fqdn.fqdn_last_ipv6,
                    fqdn_url_count=fqdn.fqdn_url_count,
                    fqdn_avg_pagerank=fqdn.fqdn_avg_pagerank,
                    fqdn_crawl_delay=fqdn.fqdn_crawl_delay,
                )
            )

        elif fqdn_exists(db, fqdn.fqdn):
            fqdn_update_list.append(
                db_models.Frontier(
                    fqdn=fqdn.fqdn,
                    tld=fqdn.tld,
                    fqdn_last_ipv4=fqdn.fqdn_last_ipv4,
                    fqdn_last_ipv6=fqdn.fqdn_last_ipv6,
                    fqdn_url_count=fqdn.fqdn_url_count,
                    fqdn_avg_pagerank=fqdn.fqdn_avg_pagerank,
                    fqdn_crawl_delay=fqdn.fqdn_crawl_delay,
                )
            )
    return fqdn_insert_list, fqdn_update_list


def save_new_fqdns(db: Session, fqdn_insert_list):
    db.bulk_save_objects(fqdn_insert_list)
    db.commit()


def update_existing_fqdns(db: Session, fqdn_update_list):
    for item in fqdn_update_list:
        fqdn = (
            db.query(db_models.Frontier)
            .filter(db_models.Frontier.fqdn == item.fqdn)
            .first()
        )

        if item.fqdn_last_ipv4 is not None:
            fqdn.fqdn_last_ipv4 = item.fqdn_last_ipv4
        if item.fqdn_last_ipv6 is not None:
            fqdn.fqdn_last_ipv6 = item.fqdn_last_ipv6
        if item.fqdn_avg_pagerank is not None:
            fqdn.fqdn_avg_pagerank = item.fqdn_avg_pagerank
        if item.fqdn_crawl_delay is not None:
            fqdn.fqdn_crawl_delay = item.fqdn_crawl_delay
        if item.fqdn_url_count is not None:
            fqdn.fqdn_url_count = item.fqdn_url_count

    db.commit()


def create_url_lists(db: Session, urls):
    url_insert_list = list()
    url_update_list = list()

    for url in urls:
        if not url_exists(db, url.url):
            url_insert_list.append(
                db_models.Url(
                    url=url.url,
                    fqdn=url.fqdn,
                    url_pagerank=url.url_pagerank,
                    url_discovery_date=url.url_discovery_date,
                    url_last_visited=url.url_last_visited,
                    url_blacklisted=url.url_blacklisted,
                    url_bot_excluded=url.url_bot_excluded,
                )
            )
        else:
            url_update_list.append(
                db_models.Url(
                    url=url.url,
                    fqdn=url.fqdn,
                    url_pagerank=url.url_pagerank,
                    url_discovery_date=url.url_discovery_date,
                    url_last_visited=url.url_last_visited,
                    url_blacklisted=url.url_blacklisted,
                    url_bot_excluded=url.url_bot_excluded,
                )
            )

    return url_insert_list, url_update_list


def save_new_urls(db: Session, url_insert_list):
    db.bulk_save_objects(url_insert_list)
    db.commit()


def update_existing_urls(db: Session, url_update_list):
    for item in url_update_list:
        url = (
            db.query(db_models.Url)
            .filter(db_models.Url.url == item.url)
            .first()
        )

        if item.url_pagerank is not None:
            url.url_pagerank = item.url_pagerank

        if item.url_discovery_date is not None:
            url.url_discovery_date = item.url_discovery_date

        if item.url_last_visited is not None:
            url.url_last_visited = item.url_last_visited

        if item.url_blacklisted is not None:
            url.url_blacklisted = item.url_blacklisted

        if item.url_bot_excluded is not None:
            url.url_bot_excluded = item.url_bot_excluded

    db.commit()


def release_fqdn_reservations(
    db: Session, uuid, fqdn_update_list: List[db_models.Frontier]
):
    for fqdn in fqdn_update_list:
        db.query(db_models.FetcherReservation).filter(
            db_models.FetcherReservation.fetcher_uuid == uuid
        ).filter(db_models.FetcherReservation.fqdn == fqdn.fqdn).delete()
    db.commit()


def commit_frontier(db: Session, submission: pyd_models.SubmitFrontier):

    fqdn_insert_list, fqdn_update_list = create_fqdn_lists(db, submission.fqdns)
    save_new_fqdns(db, fqdn_insert_list)
    update_existing_fqdns(db, fqdn_update_list)

    url_insert_list, url_update_list = create_url_lists(db, submission.urls)
    save_new_urls(db, url_insert_list)
    update_existing_urls(db, url_update_list)

    release_fqdn_reservations(db, submission.uuid, fqdn_update_list)
