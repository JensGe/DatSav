# API Endpoints
fetcher_endpoint = "/fetchers/"
database_endpoint = "/database/"
stats_endpoint = "/stats/"
frontier_endpoint = "/frontiers/"
settings_endpoint = "/settings/"
urls_endpoint = "/urls/"


# DB_Models
db_url_pk = "urls.url"
db_fqdn_pk = "frontiers.fqdn"
db_fetcher_pk = "fetcher.uuid"


# Pydantic Model Values
frontier_amount = 10
frontier_length = 0

url_frontier_count = 0
urls_count = 0

fqdn_amount = 20
fetcher = 3
min_url = 10
max_url = 100
visited_ratio = 0.0
connections = 0

# Fetcher Settings
ch_hash_amount = 32

# Frontier Settings
response_url = "http://ec2-18-195-144-15.eu-central-1.compute.amazonaws.com/submit/"
hours_to_die = 12
