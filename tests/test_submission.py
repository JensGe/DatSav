from fastapi.testclient import TestClient
from fastapi import status

from app.main import app
from app.database import database, pyd_models, db_models
from app.common import common_values as c

from tests import values as v

from pydantic import HttpUrl
from datetime import datetime
from time import sleep

example_domain_com = "www.example.com"
example_domain_de = "www.example.de"


client = TestClient(app)
db = database.SessionLocal()


def create_example_crawler():
    if not (
        db.query(db_models.Crawler)
        .filter(db_models.Crawler.uuid == v.example_uuid)
        .count()
    ):
        db.add(
            db_models.Crawler(
                uuid=v.example_uuid,
                contact="test@example.com",
                name="Test_Crawler",
                reg_date=datetime.now(),
                location="Germany",
                tld_preference="de",
            )
        )
        db.commit()


def generate_example_submission(uuid):
    frontier_one = pyd_models.UrlFrontier(
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

    frontier_two = pyd_models.UrlFrontier(
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


def get_example_submission_dict(uuid):
    return (
        {
            "uuid": uuid,
            "urls_count": 4,
            "urls": [
                {"url": "https://www.example.com/abcefg", "fqdn": example_domain_com},
                {"url": "https://www.example.com/hijklm", "fqdn": example_domain_com},
                {"url": "https://www.example.de/abcefg", "fqdn": example_domain_de},
                {"url": "https://www.example.de/hijklm", "fqdn": example_domain_de},
            ],
        },
    )


def test_unexplored_submission():
    create_example_crawler()

    submission_response = client.post(
        "/submit/", json=get_example_submission_dict(v.example_uuid)
    )

    assert submission_response.status_code == status.HTTP_202_ACCEPTED
