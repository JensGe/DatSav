from fastapi.testclient import TestClient
from fastapi import status

from app.main import app
from app.database import database, pyd_models, db_models, submit
from app.common import common_values as c

from tests import values as v

from pydantic import HttpUrl
from datetime import datetime, timezone
from time import sleep

example_domain_com = "www.example.com"
example_domain_de = "www.example.de"


client = TestClient(app)
db = database.SessionLocal()


def create_crawler(example_uuid):
    if not (
        db.query(db_models.Crawler)
        .filter(db_models.Crawler.uuid == example_uuid)
        .count()
    ):
        db.add(
            db_models.Crawler(
                uuid=example_uuid,
                contact="test@example.com",
                name="Test_Crawler",
                reg_date=datetime.now(),
                location="Germany",
                tld_preference="de",
            )
        )
        db.commit()


def create_fqdn(fqdn):
    if (
        not db.query(db_models.FqdnFrontier)
        .filter(db_models.FqdnFrontier.fqdn == fqdn)
        .count()
    ):
        db.add(db_models.FqdnFrontier(fqdn=v.example_com, tld=v.example_com[-3:]))
        db.commit()


def generate_example_submission(uuid):
    frontier_one = pyd_models.Frontier(
        fqdn=example_domain_com,
        tld="com",
        fqdn_last_ipv4="123.123.123.123",
        url_list=[
            pyd_models.Url(
                url=HttpUrl(
                    url="https://www.example.com/abcefg", scheme="http", host="example"
                ),
                fqdn=example_domain_com,
                url_last_visited=datetime.now(),
            ),
            pyd_models.Url(
                url=HttpUrl(
                    url="https://www.example.com/hijklm", scheme="http", host="example"
                ),
                fqdn=example_domain_com,
                url_last_visited=datetime.now(),
            ),
        ],
    )

    frontier_two = pyd_models.Frontier(
        fqdn=example_domain_de,
        tld="de",
        url_list=[
            pyd_models.Url(
                url=HttpUrl(
                    url="https://www.example.de/abcefg", scheme="http", host="example"
                ),
                fqdn=example_domain_de,
            ),
            pyd_models.Url(
                url=HttpUrl(
                    url="https://www.example.de/hijklm", scheme="http", host="example"
                ),
                fqdn=example_domain_de,
            ),
        ],
    )

    submit_frontier = pyd_models.SubmitFrontier(
        uuid=uuid, url_frontiers=[frontier_one, frontier_two],
    )

    return submit_frontier


def example_submission_dict():
    example_submission = {
        "uuid": v.example_uuid,
        "fqdn_count": 2,
        "fqdns": [
            {
                "fqdn": "www.example.com",
                "tld": "com",
                "fqdn_last_ipv4": "209.155.67.226",
                "fqdn_last_ipv6": "2001:DB8::6440",
                "fqdn_pagerank": 0.1,
                "fqdn_crawl_delay": 1,
                "fqdn_url_count": 5,
                "url_list": [],
            },
            {
                "fqdn": "www.example.de",
                "tld": "de",
                "fqdn_last_ipv4": "209.155.67.226",
                "fqdn_last_ipv6": "2001:DB8::6440",
                "fqdn_pagerank": 0.1,
                "fqdn_crawl_delay": 1,
                "fqdn_url_count": 5,
                "url_list": [],
            },
        ],
        "url_count": 4,
        "urls": [
            {"url": "https://www.example.com/abcefg", "fqdn": "www.example.com"},
            {"url": "https://www.example.com/hijklm", "fqdn": "www.example.com"},
            {"url": "https://www.example.de/abcefg", "fqdn": "www.example.de"},
            {"url": "https://www.example.de/hijklm", "fqdn": "www.example.de"},
        ],
    }

    return example_submission


def test_get_tld():
    fqdn1 = "www.example.com"
    fqdn2 = "www.example.de"
    fqdn3 = "www.example.co.uk"

    assert "com" == submit.get_tld(fqdn1)
    assert "de" == submit.get_tld(fqdn2)
    assert "co.uk" == submit.get_tld(fqdn3)


def test_unexplored_submission():
    create_crawler(v.example_uuid)

    submission_response = client.post(
        "/submit/",
        json={
            "uuid": v.example_uuid,
            "fqdn_count": 2,
            "fqdns": [
                {"fqdn": "www.example.de"},
                {"fqdn": "www.example.com"}
            ],
            "url_count": 4,
            "urls": [
                {"url": "https://www.example.de/abcefg", "fqdn": "www.example.de"},
                {"url": "https://www.example.de/hijklm", "fqdn": "www.example.de"},
                {"url": "https://www.example.com/abcefg", "fqdn": "www.example.com"},
                {"url": "https://www.example.com/hijklm", "fqdn": "www.example.com"},
            ],
        },
    )
    assert submission_response.status_code == status.HTTP_202_ACCEPTED


def test_duplicate_fqdn_submission():
    create_crawler(v.example_uuid)

    submission_response = client.post(
        "/submit/",
        json={
            "uuid": v.example_uuid,
            "fqdn_count": 1,
            "fqdns": [{"fqdn": "www.example.abc", "tld": "abc"}],
            "url_count": 2,
            "urls": [
                {"url": "https://www.example.abc/abcefg", "fqdn": "www.example.abc"},
                {"url": "https://www.example.abc/hijklm", "fqdn": "www.example.abc"},
            ],
        },
    )

    assert submission_response.status_code == status.HTTP_202_ACCEPTED


def test_release_fqdn_reservations():

    create_crawler(v.example_uuid)
    create_fqdn(v.example_com)

    db.add(
        db_models.CrawlerReservation(
            crawler_uuid=v.example_uuid,
            fqdn="www.example.com",
            latest_return=datetime.now(tz=timezone.utc),
        )
    )
    db.commit()

    count_before = (
        db.query(db_models.CrawlerReservation)
        .filter(db_models.CrawlerReservation.crawler_uuid == v.example_uuid)
        .count()
    )
    url_frontier = pyd_models.Frontier(fqdn="www.example.com")
    submit.release_fqdn_reservations(db, v.example_uuid, [url_frontier])

    count_after = (
        db.query(db_models.CrawlerReservation)
        .filter(db_models.CrawlerReservation.crawler_uuid == v.example_uuid)
        .count()
    )

    assert count_before == count_after + 1


def test_commit_frontier():
    create_crawler(v.example_uuid)
    response = client.post("/submit/", json=example_submission_dict())
    assert response.status_code == status.HTTP_202_ACCEPTED
