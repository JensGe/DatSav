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


def save_new_frontier(db: Session, submission: pyd_models.SubmitFrontier):

    fqdn_insert_list = [
        db_models.FqdnFrontier(
            fqdn=url_frontier.fqdn,
            tld=url_frontier.tld,
            fqdn_last_ipv4=url_frontier.fqdn_last_ipv4,
            fqdn_last_ipv6=url_frontier.fqdn_last_ipv6,
            fqdn_pagerank=url_frontier.fqdn_pagerank,
            fqdn_crawl_delay=url_frontier.fqdn_crawl_delay,
            fqdn_url_count=url_frontier.fqdn_url_count,
        )
        for url_frontier in submission.url_frontiers
        if not fqdn_exists(db, url_frontier.fqdn)
    ]

    db.bulk_save_objects(fqdn_insert_list)
    db.commit()

    fqdn_update_list = [
        db_models.FqdnFrontier(
            fqdn=url_frontier.fqdn,
            tld=url_frontier.tld,
            fqdn_last_ipv4=url_frontier.fqdn_last_ipv4,
            fqdn_last_ipv6=url_frontier.fqdn_last_ipv6,
            fqdn_pagerank=url_frontier.fqdn_pagerank,
            fqdn_crawl_delay=url_frontier.fqdn_crawl_delay,
            fqdn_url_count=url_frontier.fqdn_url_count,
        )
        for url_frontier in submission.url_frontiers
        if fqdn_exists(db, url_frontier.fqdn)
    ]

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

    url_insert_list = []
    url_update_list = []

    for url_frontier in submission.url_frontiers:
        for url in url_frontier.url_list:
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

    db.bulk_save_objects(url_insert_list)
    db.commit()

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
