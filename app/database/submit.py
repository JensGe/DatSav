from app.database import pyd_models, db_models

from sqlalchemy.orm import Session


def fqdn_exists(db: Session, fqdn):
    if (
        db.query(db_models.FqdnFrontier)
        .filter(db_models.FqdnFrontier.fqdn == fqdn)
        .count()
        > 0
    ):
        return True
    return False


def url_exists(db: Session, url):
    if (
        db.query(db_models.UrlFrontier).filter(db_models.UrlFrontier.url == url).count()
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


def create_fqdn_lists(db: Session, urls):
    fqdn_insert_list = list()
    fqdn_update_list = list()

    for url in urls:
        if not fqdn_exists(db, url.fqdn) and url.fqdn not in [
            url.fqdn for url in fqdn_insert_list
        ]:
            fqdn_insert_list.append(
                db_models.FqdnFrontier(fqdn=url.fqdn, tld=get_tld(url.fqdn))
            )

        elif fqdn_exists(db, url.fqdn):
            fqdn_update_list.append(
                db_models.FqdnFrontier(fqdn=url.fqdn, tld=get_tld(url.fqdn))
            )
    return fqdn_insert_list, fqdn_update_list


def save_new_fqdns(db: Session, fqdn_insert_list):
    db.bulk_save_objects(fqdn_insert_list)
    db.commit()


def update_existing_fqdns(db: Session, fqdn_update_list):
    for item in fqdn_update_list:
        db.query(db_models.FqdnFrontier).filter(
            db_models.FqdnFrontier.fqdn == item.fqdn
        ).update(
            {
                db_models.FqdnFrontier.fqdn_last_ipv4: item.fqdn_last_ipv4,
                db_models.FqdnFrontier.fqdn_last_ipv6: item.fqdn_last_ipv6,
                db_models.FqdnFrontier.fqdn_pagerank: item.fqdn_pagerank,
                db_models.FqdnFrontier.fqdn_crawl_delay: item.fqdn_crawl_delay,
                db_models.FqdnFrontier.fqdn_url_count: item.fqdn_url_count,
            }
        )
    db.commit()


def create_url_lists(db: Session, urls):
    url_insert_list = list()
    url_update_list = list()

    for url in urls:
        if not url_exists(db, url.url):
            url_insert_list.append(
                db_models.UrlFrontier(
                    url=url.url,
                    fqdn=url.fqdn,
                    url_discovery_date=url.url_discovery_date,
                    url_last_visited=url.url_last_visited,
                    url_blacklisted=url.url_blacklisted,
                    url_bot_excluded=url.url_bot_excluded,
                )
            )
        else:
            url_update_list.append(
                db_models.UrlFrontier(
                    url=url.url,
                    fqdn=url.fqdn,
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
        db.query(db_models.UrlFrontier).filter(
            db_models.UrlFrontier.url == item.url
        ).update(
            {
                db_models.UrlFrontier.url_discovery_date: item.url_discovery_date,
                db_models.UrlFrontier.url_last_visited: item.url_last_visited,
                db_models.UrlFrontier.url_blacklisted: item.url_blacklisted,
                db_models.UrlFrontier.url_bot_excluded: item.url_bot_excluded,
            }
        )

    db.commit()


def release_fqdn_reservations(db: Session, uuid, fqdn_update_list):
    for fqdn in fqdn_update_list:
        db.query(db_models.CrawlerReservation).filter(
            db_models.CrawlerReservation.crawler_uuid == uuid
        ).filter(db_models.CrawlerReservation.fqdn == fqdn.fqdn).delete()
    db.commit()
    

def commit_frontier(db: Session, submission: pyd_models.SubmitFrontier):

    fqdn_insert_list, fqdn_update_list = create_fqdn_lists(db, submission.urls)
    save_new_fqdns(db, fqdn_insert_list)
    update_existing_fqdns(db, fqdn_update_list)

    url_insert_list, url_update_list = create_url_lists(db, submission.urls)
    save_new_urls(db, url_insert_list)
    update_existing_urls(db, url_update_list)

    release_fqdn_reservations(db, submission.uuid, fqdn_update_list)
